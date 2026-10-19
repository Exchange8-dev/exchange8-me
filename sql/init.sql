-- SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
-- SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
-- SPDX-FileCopyrightText: Copyright Contributors to the Exchange8:ME
-- SPDX-License-Identifier: Apache-2.0

-- Ex8:ME - auction sessions and participating orders.
--
-- Design: design/sentin8-exchange8-data-model-technical-design.md §3.11 (ME).
-- No migration code (new platform): `sentin8-cli vertical init` applies this.

CREATE SCHEMA IF NOT EXISTS me;

-- Auction session (one per book + type + day).
CREATE TABLE IF NOT EXISTS me.auction (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    env_id      UUID NOT NULL,
    book_id     UUID NOT NULL,           -- RD-owned refdata.book
    security_id UUID,
    auction_type TEXT NOT NULL CHECK (auction_type IN
                ('opening','closing','reopen','halt','ipo','volatility')),
    state       TEXT NOT NULL DEFAULT 'scheduled' CHECK (state IN
                ('scheduled','accepting','locked','uncrossing','complete','cancelled')),
    uncross_price NUMERIC(18,8),
    scheduled_at TIMESTAMPTZ NOT NULL,
    uncrossed_at  TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_exchange8_auction_env ON me.auction (env_id);

-- Auction-participating orders (snapshot at uncross).
CREATE TABLE IF NOT EXISTS me.auction_order (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    env_id      UUID NOT NULL,
    auction_id  UUID NOT NULL REFERENCES me.auction(id),
    order_id    BIGINT NOT NULL,
    side        CHAR(1) NOT NULL CHECK (side IN ('B','S')),
    price       NUMERIC(18,8),
    qty         NUMERIC(18,8) NOT NULL,
    filled_qty   NUMERIC(18,8) NOT NULL DEFAULT 0
);

-- ---------------------------------------------------------------------------
-- RLS (architecture-principles.md §4.3): enable + force on all tenant tables.
-- Policies use the platform helpers sentin8_context_initialized() and
-- sentin8_current_env_id(); services call set_tenant_context on the same
-- connection as the data query.
-- ---------------------------------------------------------------------------
DO $$
DECLARE tbl TEXT;
BEGIN
    FOREACH tbl IN ARRAY ARRAY['me.auction', 'me.auction_order'] LOOP
        BEGIN
            EXECUTE format('ALTER TABLE %s ENABLE ROW LEVEL SECURITY', tbl);
            EXECUTE format('ALTER TABLE %s FORCE ROW LEVEL SECURITY', tbl);
            EXECUTE format('DROP POLICY IF EXISTS %1$I_select ON %2$s;
CREATE POLICY %1$I_select ON %2$s FOR SELECT USING (sentin8_context_initialized() AND env_id = sentin8_current_env_id())', split_part(tbl, '.', 2), tbl);
            EXECUTE format('DROP POLICY IF EXISTS %1$I_insert ON %2$s;
CREATE POLICY %1$I_insert ON %2$s FOR INSERT WITH CHECK (sentin8_context_initialized() AND env_id = sentin8_current_env_id())', split_part(tbl, '.', 2), tbl);
            EXECUTE format('DROP POLICY IF EXISTS %1$I_update ON %2$s;
CREATE POLICY %1$I_update ON %2$s FOR UPDATE USING (sentin8_context_initialized() AND env_id = sentin8_current_env_id())', split_part(tbl, '.', 2), tbl);
            EXECUTE format('DROP POLICY IF EXISTS %1$I_delete ON %2$s;
CREATE POLICY %1$I_delete ON %2$s FOR DELETE USING (sentin8_context_initialized() AND env_id = sentin8_current_env_id())', split_part(tbl, '.', 2), tbl);
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'RLS setup failed for %: %', tbl, SQLERRM;
        END;
    END LOOP;
END $$;

-- ---------------------------------------------------------------------------
-- Per-service role grants (schema-per-service: this service owns schema
-- `me`; grants live in db `exchange8` only - design doc §1).
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'exchange8_me_service') THEN
        ALTER ROLE exchange8_me_service SET search_path = me, public;
        GRANT USAGE ON SCHEMA me TO exchange8_me_service;
        GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA me TO exchange8_me_service;
        GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA me TO exchange8_me_service;
    END IF;
END $$;

-- ---------------------------------------------------------------------------
-- Local outbox (DBX §6.7 - generated from `sentin8_outbox::outbox_table_sql(
-- "me")`): rows written in the same DB transaction as the domain change;
-- the shell's relay (boot step §8b) pushes them to Bus `POST /v1/produce`.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS me.outbox (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    env_id          UUID NOT NULL,
    event_type      TEXT NOT NULL,
    stream_family   TEXT NOT NULL,
    payload         JSONB NOT NULL,
    publisher_id_token  TEXT,
    publisher_subject   TEXT,
    relayed         BOOLEAN NOT NULL DEFAULT false,
    relayed_at      TIMESTAMPTZ,
    relay_attempts  INT NOT NULL DEFAULT 0,
    next_retry_at   TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_me_outbox_unrelayed
    ON me.outbox (created_at) WHERE relayed = false;
DO $$ BEGIN
    EXECUTE 'ALTER TABLE me.outbox ENABLE ROW LEVEL SECURITY';
    EXECUTE 'ALTER TABLE me.outbox FORCE ROW LEVEL SECURITY';
    EXECUTE 'DROP POLICY IF EXISTS outbox_select ON me.outbox';
    EXECUTE 'CREATE POLICY outbox_select ON me.outbox FOR SELECT
        USING (sentin8_context_initialized()
               AND env_id = sentin8_current_env_id())';
    EXECUTE 'DROP POLICY IF EXISTS outbox_insert ON me.outbox';
    EXECUTE 'CREATE POLICY outbox_insert ON me.outbox FOR INSERT
        WITH CHECK (sentin8_context_initialized()
               AND env_id = sentin8_current_env_id())';
    EXECUTE 'DROP POLICY IF EXISTS outbox_update ON me.outbox';
    EXECUTE 'CREATE POLICY outbox_update ON me.outbox FOR UPDATE
        USING (sentin8_context_initialized()
               AND env_id = sentin8_current_env_id())';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'RLS setup failed for me.outbox: %', SQLERRM;
END $$;

CREATE OR REPLACE FUNCTION me.me_notify_outbox() RETURNS TRIGGER
LANGUAGE plpgsql AS $fn$
BEGIN
    PERFORM pg_notify('me_outbox_changed', '');
    RETURN NEW;
END;
$fn$;
DROP TRIGGER IF EXISTS outbox_notify_trigger ON me.outbox;
CREATE TRIGGER outbox_notify_trigger
    AFTER INSERT ON me.outbox
    FOR EACH ROW
    EXECUTE FUNCTION me.me_notify_outbox();

-- ---------------------------------------------------------------------------
-- Local audit journal (DBX §6.8 - generated from
-- `sentin8_audit::audit_journal_sql("me")`): rows written in the same
-- transaction as the domain change; the shell's pusher (boot step §8c) pushes
-- them to Sys `POST /v1/audit` (`sentin8:sys:audit:write`). A Sys outage
-- never drops audit.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS me.audit_journal (
    id              UUID PRIMARY KEY,
    env_id          UUID NOT NULL,
    action          TEXT NOT NULL,
    target_resource_type TEXT,
    target_resource_id    TEXT,
    actor_user_id   TEXT,
    actor_subject_type TEXT,
    severity        TEXT NOT NULL DEFAULT 'info',
    correlation_id  TEXT,
    before_state    JSONB,
    after_state     JSONB,
    details         JSONB,
    pushed          BOOLEAN NOT NULL DEFAULT false,
    pushed_at       TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_me_audit_journal_unpushed
    ON me.audit_journal (created_at) WHERE pushed = false;
DO $$ BEGIN
    EXECUTE 'ALTER TABLE me.audit_journal ENABLE ROW LEVEL SECURITY';
    EXECUTE 'ALTER TABLE me.audit_journal FORCE ROW LEVEL SECURITY';
    EXECUTE 'DROP POLICY IF EXISTS audit_journal_dml ON me.audit_journal';
    EXECUTE 'CREATE POLICY audit_journal_dml ON me.audit_journal
        USING (sentin8_context_initialized()
               AND env_id = sentin8_current_env_id())
        WITH CHECK (sentin8_context_initialized()
               AND env_id = sentin8_current_env_id())';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'RLS setup failed for me.audit_journal: %', SQLERRM;
END $$;

-- Outbox + journal role grants (same pattern as the table grants above).
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'exchange8_me_service') THEN
        GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA me TO exchange8_me_service;
    END IF;
END $$;
