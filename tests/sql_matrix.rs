// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Exchange8:ME
// SPDX-License-Identifier: Apache-2.0

//! Exchange8:ME SQL integration tests — RLS matrix replica
//! (design §13b.1).
//!
//! Runs against the live platform PostgreSQL (18) as the `exchange8_probe`
//! NOBYPASSRLS role (`sql/probe.sql`) — RLS is meaningful only when the
//! caller cannot bypass it. The matrix:
//!   1. every table in THIS service's own `sql/init.sql` has FORCE RLS +
//!      a select policy (self-maintaining: derived from the DDL the
//!      binary ships)
//!   2. no tenant context → 0 rows (silent RLS enforcement)
//!   3. insert without tenant context → rejected
//!   4. own-env context → probe row visible exactly once
//!   5. cross-env context → 0 rows
//!
//! All fixture writes ride ONE rolled-back transaction — zero residue on
//! the live schema. Skips silently when the DB is unreachable so
//! `cargo test` stays hermetic offline.
//!
//! Design: `design/sentin8-exchange8-technical-design.md` §13b.1;
//! pattern origin: `services/settlement/tests/sql_matrix.rs`.

use sqlx::{Connection, PgPool};
use uuid::Uuid;

const PROBE_URL: &str = "postgresql://exchange8_probe@localhost:5432/exchange8";

/// This service's own schema DDL — the policy-coverage source of truth.
const INIT_SQL: &str = include_str!("../sql/init.sql");

/// INSERT fixture ($1 = env_id).
const INSERT_SQL: &str = "INSERT INTO me.auction (env_id, book_id, auction_type, scheduled_at) VALUES ($1, '99999999-9999-9999-9999-999999999901', 'opening', now())";
/// Marker predicate counting exactly the probe rows (policy-filtered).
const COUNT_SQL: &str =
    "SELECT count(*) FROM me.auction WHERE book_id = '99999999-9999-9999-9999-999999999901'";

fn env_a() -> Uuid {
    Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap()
}

fn env_b() -> Uuid {
    Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap()
}

/// ONE connection per test — tenant context is connection-local
/// (`set_config(..., true)`); context-set and data queries ride the same
/// connection (architecture-principles.md §4.3d). `None` → skip (offline).
async fn probe_conn() -> Option<sqlx::pool::PoolConnection<sqlx::Postgres>> {
    let pool = PgPool::connect(PROBE_URL).await.ok()?;
    pool.acquire().await.ok()
}

async fn set_ctx(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, env: Uuid) {
    sqlx::query("SELECT sentin8_set_tenant_context($1)")
        .bind(env)
        .execute(&mut **tx)
        .await
        .unwrap();
}

/// Tables this service's init.sql declares.
fn declared_tables() -> Vec<String> {
    INIT_SQL
        .lines()
        .filter_map(|l| l.trim_start().strip_prefix("CREATE TABLE IF NOT EXISTS "))
        .map(|rest| rest.split_whitespace().next().unwrap().to_string())
        .collect()
}

/// 1 — policy coverage: every declared table is RLS-enforced.
#[tokio::test]
async fn every_owned_table_has_force_rls_and_select_policy() {
    let Some(mut conn) = probe_conn().await else {
        eprintln!("skipping: no live DB");
        return;
    };
    let tables = declared_tables();
    assert!(!tables.is_empty(), "init.sql declares no tables");
    for t in &tables {
        let (schema, name) = t.split_once('.').expect("schema-qualified table");
        let row: Option<(bool, bool)> = sqlx::query_as(
            "SELECT relrowsecurity, relforcerowsecurity \
             FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = $1 AND c.relname = $2",
        )
        .bind(schema)
        .bind(name)
        .fetch_one(&mut *conn)
        .await
        .ok();
        let (enabled, forced) = row.unwrap_or_else(|| {
            panic!("table {t} (from this service's init.sql) does not exist live — vertical init not applied?");
        });
        assert!(enabled, "{t}: RLS not enabled");
        assert!(forced, "{t}: RLS not FORCEd (owner could bypass)");
        let policies: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_policies WHERE schemaname=$1 AND tablename=$2",
        )
        .bind(schema)
        .bind(name)
        .fetch_one(&mut *conn)
        .await
        .unwrap();
        assert!(policies > 0, "{t}: no RLS policies");
    }
}

/// 2/3/4/5 — the behavioral matrix on the representative table.
#[tokio::test]
async fn rls_matrix_on_representative_table() {
    let Some(mut conn) = probe_conn().await else {
        eprintln!("skipping: no live DB");
        return;
    };
    let mut tx = conn.begin().await.unwrap();

    // 2 — no context → 0 rows (silent enforcement, not an error).
    let n: i64 = sqlx::query_scalar(COUNT_SQL)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert_eq!(n, 0, "no tenant context must see zero rows");

    // 3 — insert without context → rejected by the WITH CHECK policy.
    // SAVEPOINT: an RLS rejection aborts the surrounding transaction
    // (25P02); roll back to the savepoint so steps 4-5 stay runnable.
    sqlx::query("SAVEPOINT no_ctx")
        .execute(&mut *tx)
        .await
        .unwrap();
    let res = sqlx::query(INSERT_SQL)
        .bind(env_a())
        .execute(&mut *tx)
        .await;
    assert!(
        res.is_err(),
        "insert without tenant context must be rejected"
    );
    sqlx::query("ROLLBACK TO SAVEPOINT no_ctx")
        .execute(&mut *tx)
        .await
        .unwrap();

    // 4 — own-env context → probe row visible.
    set_ctx(&mut tx, env_a()).await;
    sqlx::query(INSERT_SQL)
        .bind(env_a())
        .execute(&mut *tx)
        .await
        .unwrap();
    let n: i64 = sqlx::query_scalar(COUNT_SQL)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert_eq!(n, 1, "own env must see exactly its probe row");

    // 5 — cross-env context → 0 rows (env_a row invisible from env_b).
    set_ctx(&mut tx, env_b()).await;
    let n: i64 = sqlx::query_scalar(COUNT_SQL)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert_eq!(n, 0, "cross-env must not see the probe row");

    tx.rollback().await.unwrap();
}
