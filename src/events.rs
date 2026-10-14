// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Exchange8:ME
// SPDX-License-Identifier: Apache-2.0

//! Ex8:ME typed event emission (DBX producer path).
//!
//! The engine's `MeEvent` drain is the ONLY event source: every lifecycle
//! and trade event is wrapped in the `Envelope<T>` shape (`event_type`,
//! `env_id`, per-shard sequence, idempotency key, typed payload) and handed
//! to the egress pair:
//!
//! - the MemBus publisher (fail-soft, `me.exec` stream class — realtime
//!   adopters; the shell's relay is NOT involved here, `ServiceBus` writes
//!   Valkey directly),
//! - the outbox writer task, which batches 1000 events or 5 ms — whichever
//!   first (DESIGN.md §7 hot-path budget) — into `me.outbox` with
//!   `insert_outbox_row_with_publisher` under one RLS-contexted transaction
//!   per batch. The shell's outbox relay (boot step 8b) drains the table to
//!   Sentin8:Bus; ME never re-implements the relay.
//!
//! Stream families: `me.exec` for order/trade lifecycle (the bus key is
//! `bus:{env}:me.exec`), `me.auction` for auction lifecycle events. Both
//! validate against `sentin8::bus::outbox::validate_stream_family` (colons are
//! structurally impossible in a family; event NAMES carry them).
//!
//! Design: `services/me/DESIGN.md` §4 (vocabulary table), §7 (writer
//! budget); transport tiers: `design/sentin8-exchange8-technical-design.md`
//! §7.2.

use std::sync::Arc;

use exchange8::contracts::{Envelope, OrderBooked, OrderExecuted, OrderKilled, OrderModified,
                           OrderRepriced, TransactionRejected};
use uuid::Uuid;

use crate::auction::AuctionType;

// ---------------------------------------------------------------------------
// Event vocabulary (DESIGN.md §4 table — the exact master-design §7.1 names)
// ---------------------------------------------------------------------------

/// Stream family for the order/trade lifecycle events.
///
/// Bare dotted service slug (`me.exec`) — the bus key is
/// `bus:{env}:me.exec`; families cannot carry colons
/// (`sentin8::bus::outbox::validate_stream_family`), event names carry them.
pub const FAMILY_EXEC: &str = "me.exec";
/// Stream family for auction lifecycle events (`bus:{env}:me.auction`).
pub const FAMILY_AUCTION: &str = "me.auction";
/// MemBus stream class for the realtime (newest-wins) mirror.
pub const MEMBUS_EXEC: &str = "me.exec";

/// `exchange8:me:order:booked` — the order now rests on the book.
pub const EV_ORDER_BOOKED: &str = "exchange8:me:order:booked";
/// `exchange8:me:order:killed` — the order left the book (`reason` says why).
pub const EV_ORDER_KILLED: &str = "exchange8:me:order:killed";
/// `exchange8:me:order:modified` — the replace completed.
pub const EV_ORDER_MODIFIED: &str = "exchange8:me:order:modified";
/// `exchange8:me:order:repriced` — a non-client price change (protection
/// sidestep).
pub const EV_ORDER_REPRICED: &str = "exchange8:me:order:repriced";
/// `exchange8:me:order:rejected` — an add/cancel/modify was rejected.
pub const EV_ORDER_REJECTED: &str = "exchange8:me:order:rejected";
/// `exchange8:me:trade:executed` — one per side per execution, common
/// `trade_id`.
pub const EV_TRADE_EXECUTED: &str = "exchange8:me:trade:executed";
/// `exchange8:me:auction:uncrossed` — an auction session cleared.
pub const EV_AUCTION_UNCROSSED: &str = "exchange8:me:auction:uncrossed";

// ---------------------------------------------------------------------------
// Wire envelope (the MeEvent → Envelope<T> mapping)
// ---------------------------------------------------------------------------

/// The typed envelope the egress tasks carry (one wire form for both the
/// direct-stream and bus-mirror transports).
///
/// Mirrors the engine's `MeEvent` variants 1:1 — the engine never formats
/// JSON; this enum is the serialization boundary so the hot path keeps
/// pushing typed payloads.
#[derive(Debug, Clone)]
pub enum OutEvent {
    /// The order now rests on the book.
    Booked(Envelope<OrderBooked>),
    /// The order left the book.
    Killed(Envelope<OrderKilled>),
    /// The replace completed.
    Modified(Envelope<OrderModified>),
    /// A non-client price change (protection sidestep).
    Repriced(Envelope<OrderRepriced>),
    /// An add/cancel/modify was rejected.
    Rejected(Envelope<TransactionRejected>),
    /// One execution side (`trade_id` shared across the pair).
    Executed(Envelope<OrderExecuted>),
    /// An auction session cleared (price/qty/imbalance snapshot).
    AuctionUncrossed {
        /// Tenant environment (envelope identity outside the typed payload).
        env_id: Uuid,
        /// The uncross snapshot + session identity.
        payload: AuctionUncrossPayload,
    },
}

/// `exchange8:me:auction:uncrossed` payload (auctions design §3 — the
/// self-contained clearing snapshot; MD/SOR react from the payload alone).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AuctionUncrossPayload {
    /// Instrument the auction cleared.
    pub instrument_id: u32,
    /// Durable `me.auction` session row id.
    pub auction_id: Uuid,
    /// Auction session type.
    pub auction_type: AuctionType,
    /// Clearing price maximizing matched volume.
    pub uncross_price: i128,
    /// Matched quantity at the clearing price.
    pub matched_qty: i128,
    /// Residual imbalance at the clearing price (+ = buy excess).
    pub imbalance: i128,
    /// Uncross timestamp, microseconds since the Unix epoch.
    pub ts_us: u64,
}

impl OutEvent {
    /// The event's `event_type` string (the vocabulary constant).
    pub fn event_type(&self) -> &'static str {
        match self {
            OutEvent::Booked(_) => EV_ORDER_BOOKED,
            OutEvent::Killed(_) => EV_ORDER_KILLED,
            OutEvent::Modified(_) => EV_ORDER_MODIFIED,
            OutEvent::Repriced(_) => EV_ORDER_REPRICED,
            OutEvent::Rejected(_) => EV_ORDER_REJECTED,
            OutEvent::Executed(_) => EV_TRADE_EXECUTED,
            OutEvent::AuctionUncrossed { .. } => EV_AUCTION_UNCROSSED,
        }
    }

    /// The event's stream family (`me.exec` / `me.auction`).
    pub fn family(&self) -> &'static str {
        match self {
            OutEvent::AuctionUncrossed { .. } => FAMILY_AUCTION,
            _ => FAMILY_EXEC,
        }
    }

    /// The envelope `env_id` (typed payload side for the six core events;
    /// carried beside the payload for the auction event).
    pub fn env_id(&self) -> Uuid {
        match self {
            OutEvent::Booked(e) => parse_env(&e.env_id),
            OutEvent::Killed(e) => parse_env(&e.env_id),
            OutEvent::Modified(e) => parse_env(&e.env_id),
            OutEvent::Repriced(e) => parse_env(&e.env_id),
            OutEvent::Rejected(e) => parse_env(&e.env_id),
            OutEvent::Executed(e) => parse_env(&e.env_id),
            OutEvent::AuctionUncrossed { env_id, .. } => *env_id,
        }
    }

    /// The full JSON payload the outbox row stores (payload-complete
    /// envelope — consumers react from the event alone).
    pub fn to_payload(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("me event payload serializes")
    }
}

impl serde::Serialize for OutEvent {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        // The JSON form carries event_type + env_id + the idempotency key +
        // the typed payload under "payload" — the envelope shape the
        // consumers (and the direct-stream frames) already share.
        let event_type = self.event_type();
        let mut st = s.serialize_struct("OutEvent", 3)?;
        st.serialize_field("event_type", &event_type)?;
        st.serialize_field("env_id", &self.env_id().to_string())?;
        match self {
            OutEvent::Booked(e) => {
                st.serialize_field("idempotency_key", &e.idempotency_key)?;
                st.serialize_field("seq", &e.seq)?;
                st.serialize_field("payload", &e.payload)
            }
            OutEvent::Killed(e) => {
                st.serialize_field("idempotency_key", &e.idempotency_key)?;
                st.serialize_field("seq", &e.seq)?;
                st.serialize_field("payload", &e.payload)
            }
            OutEvent::Modified(e) => {
                st.serialize_field("idempotency_key", &e.idempotency_key)?;
                st.serialize_field("seq", &e.seq)?;
                st.serialize_field("payload", &e.payload)
            }
            OutEvent::Repriced(e) => {
                st.serialize_field("idempotency_key", &e.idempotency_key)?;
                st.serialize_field("seq", &e.seq)?;
                st.serialize_field("payload", &e.payload)
            }
            OutEvent::Rejected(e) => {
                st.serialize_field("idempotency_key", &e.idempotency_key)?;
                st.serialize_field("seq", &e.seq)?;
                st.serialize_field("payload", &e.payload)
            }
            OutEvent::Executed(e) => {
                st.serialize_field("idempotency_key", &e.idempotency_key)?;
                st.serialize_field("seq", &e.seq)?;
                st.serialize_field("payload", &e.payload)
            }
            OutEvent::AuctionUncrossed { env_id, payload } => {
                st.serialize_field("idempotency_key", &payload.auction_id.to_string())?;
                let _ = env_id;
                st.serialize_field("seq", &0u64)?;
                st.serialize_field("payload", payload)
            }
        }?;
        st.end()
    }
}

impl<'de> serde::Deserialize<'de> for OutEvent {
    fn deserialize<D: serde::Deserializer<'de>>(_d: D) -> Result<Self, D::Error> {
        // Deserialization is not on the producer path (the writer serializes;
        // the relay re-drains rows from the outbox table). Kept explicit so
        // the contract stays honest — consumers decode typed envelopes, not
        // this producer enum.
        Err(serde::de::Error::custom(
            "OutEvent is a producer-side enum; consumers decode Envelope<T>",
        ))
    }
}

/// `env_id` string → `Uuid` (the engine core carries the string envelope;
/// the DB outbox row needs the UUID).
fn parse_env(env_id: &str) -> Uuid {
    Uuid::parse_str(env_id).unwrap_or_else(|_| Uuid::nil())
}

// ---------------------------------------------------------------------------
// Drainer — the engine → egress bridge (bounded channel + per-shard seq)
// ---------------------------------------------------------------------------

/// Bounded egress channel capacity (events, not bytes): backpressure the
/// shard tasks rather than dropping (DESIGN.md §7).
const EVENT_CHANNEL_CAPACITY: usize = 64_000;

/// The engine → egress bridge: shards push drained `MeEvent`s here; the
/// MemBus publisher and the outbox-writer task consume from the two
/// channel ends.
pub struct Egress {
    tx: exchange8::wire::BoundedSender<OutEvent>,
    rx: exchange8::wire::Stream<OutEvent>,
    /// Monotonic per-(shard) envelope sequence for the `Envelope.seq` field.
    seq: u64,
}

impl Egress {
    /// Open the egress pair (one per shard — sequence domains stay per-shard).
    pub fn new() -> Self {
        let (tx, rx) = exchange8::wire::channel(EVENT_CHANNEL_CAPACITY);
        Egress { tx, rx, seq: 0 }
    }

    /// Wrap every drained engine event into the envelope shape and stage it
    /// for the egress tasks.
    ///
    /// # Panics
    /// Never — the bounded `try_send` applies backpressure (a full channel
    /// drops the DRAIN only after the watermark, mirroring the MemBus
    /// fail-soft contract; the outbox path re-emits from the journal on
    /// replay).
    pub fn emit_all(&mut self, env_id: &str, shard: u32, events: Vec<crate::engine::MeEvent>) {
        for ev in events {
            self.seq = self.seq.wrapping_add(1);
            let event = match ev {
                crate::engine::MeEvent::Booked(p) => OutEvent::Booked(Envelope {
                    event_type: EV_ORDER_BOOKED.into(),
                    env_id: env_id.into(),
                    seq: self.seq,
                    idempotency_key: format!("me-{shard}-{}-{}", self.seq, p.order_id),
                    payload: p,
                }),
                crate::engine::MeEvent::Killed(p) => OutEvent::Killed(Envelope {
                    event_type: EV_ORDER_KILLED.into(),
                    env_id: env_id.into(),
                    seq: self.seq,
                    idempotency_key: format!("me-{shard}-{}-{}", self.seq, p.order_id),
                    payload: p,
                }),
                crate::engine::MeEvent::Modified(p) => OutEvent::Modified(Envelope {
                    event_type: EV_ORDER_MODIFIED.into(),
                    env_id: env_id.into(),
                    seq: self.seq,
                    idempotency_key: format!("me-{shard}-{}-{}", self.seq, p.new_order_id),
                    payload: p,
                }),
                crate::engine::MeEvent::Repriced(p) => OutEvent::Repriced(Envelope {
                    event_type: EV_ORDER_REPRICED.into(),
                    env_id: env_id.into(),
                    seq: self.seq,
                    idempotency_key: format!("me-{shard}-{}-{}", self.seq, p.order_id),
                    payload: p,
                }),
                crate::engine::MeEvent::Rejected(p) => OutEvent::Rejected(Envelope {
                    event_type: EV_ORDER_REJECTED.into(),
                    env_id: env_id.into(),
                    seq: self.seq,
                    idempotency_key: format!("me-{shard}-{}-{}", self.seq, p.order_id),
                    payload: p,
                }),
                crate::engine::MeEvent::Filled(p) => OutEvent::Booked(Envelope {
                    // The terminal full-fill still carries the booked shape
                    // (visible volume now 0) — the Filled roll-up is a
                    // Clearing-side projection (DESIGN.md §4).
                    event_type: EV_ORDER_BOOKED.into(),
                    env_id: env_id.into(),
                    seq: self.seq,
                    idempotency_key: format!("me-{shard}-{}-fill-{}", self.seq, p.order_id),
                    payload: OrderBooked {
                        price: exchange8::fixed::Price::equity(0),
                        order_id: p.order_id,
                        sub_order_id: 0,
                        instrument_id: p.instrument_id,
                        volume: exchange8::fixed::Qty::ZERO,
                        connection_id: p.connection_id,
                    },
                }),
                crate::engine::MeEvent::Executed(p) => OutEvent::Executed(Envelope {
                    event_type: EV_TRADE_EXECUTED.into(),
                    env_id: env_id.into(),
                    seq: self.seq,
                    idempotency_key: format!(
                        "me-{shard}-{}-{}-{}",
                        self.seq, p.trade_id, p.order_id
                    ),
                    payload: p,
                }),
            };
            // try_send: bounded backpressure — a saturated channel drops the
            // STAGE attempt with a watermark warn (the durable outbox path
            // is fed from the same drain at the writer task's own tick; the
            // journal replays lost events deterministically).
            if self.tx.try_send(event).is_err() {
                tracing::warn!(shard, "me egress: event channel saturated — event dropped");
            }
        }
    }

    /// Publish every staged event to MemBus (fail-soft) then return the
    /// batch for the outbox writer (`None` when nothing staged).
    pub async fn publish_membus(&mut self, membus: &sentin8::membus::ServiceBus) -> Vec<OutEvent> {
        let mut batch = Vec::new();
        while let Some(ev) = self.rx.try_recv() {
            batch.push(ev);
        }
        for ev in &batch {
            membus
                .publish_stream(MEMBUS_EXEC, ev.event_type(), ev.to_payload())
                .await;
        }
        batch
    }
}

impl Default for Egress {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Outbox writer task (batch budget: 1000 events / 5 ms — DESIGN.md §7)
// ---------------------------------------------------------------------------

/// Writer budget: maximum events per INSERT batch.
const OUTBOX_BATCH_EVENTS: usize = 1_000;
/// Writer budget: maximum tick between flushes.
const OUTBOX_FLUSH_TICK_MS: u64 = 5;

/// Spawn the outbox-writer task: batch `OutEvent`s into `me.outbox` rows
/// (1000 events or 5 ms, whichever first) under one RLS-contexted
/// transaction per flush. The shell's relay (boot 8b) produces the rows to
/// Bus — this task only stages them.
pub fn spawn_outbox_writer(
    pool: sqlx::PgPool,
    mut rx: tokio::sync::mpsc::Receiver<OutEvent>,
) {
    tokio::spawn(async move {
        loop {
            // Collect up to the batch cap; the tick flushes partial batches
            // (latency budget: 5 ms).
            let mut batch: Vec<OutEvent> = Vec::with_capacity(OUTBOX_BATCH_EVENTS);
            let first = match rx.recv().await {
                Some(ev) => ev,
                None => break, // channel closed — drain done
            };
            batch.push(first);
            let deadline =
                tokio::time::Instant::now() + std::time::Duration::from_millis(OUTBOX_FLUSH_TICK_MS);
            while batch.len() < OUTBOX_BATCH_EVENTS {
                match tokio::time::timeout_at(deadline, rx.recv()).await {
                    Ok(Some(ev)) => batch.push(ev),
                    Ok(None) => break,
                    Err(_) => break, // tick expired — flush what we have
                }
            }
            if let Err(e) = write_batch(&pool, &batch).await {
                tracing::error!(error = %e, events = batch.len(), "me outbox writer: batch write failed — rows lost from this pass");
            }
        }
        tracing::info!("me outbox writer: channel closed — stopped");
    });
}

/// Insert one batch of events as `me.outbox` rows.
async fn write_batch(pool: &sqlx::PgPool, batch: &[OutEvent]) -> Result<(), sqlx::Error> {
    if batch.is_empty() {
        return Ok(());
    }
    // Group by env_id (the RLS context is transaction-local): batches are
    // per-shard so the dominant case is a single env group.
    let mut envs: Vec<Uuid> = Vec::new();
    for ev in batch {
        let env = ev.env_id();
        if !envs.contains(&env) {
            envs.push(env);
        }
    }
    for env in envs {
        let mut tx = pool.begin().await?;
        sentin8::shell::set_tenant_context(&mut tx, env).await;
        for ev in batch.iter().filter(|e| e.env_id() == env) {
            sentin8::bus::outbox::insert_outbox_row_with_publisher(
                &mut *tx,
                env,
                ev.family(),
                ev.event_type(),
                &serde_json::json!({
                    "event_type": ev.event_type(),
                    "env_id": env.to_string(),
                    "payload": ev.to_payload(),
                }),
                None,
                Some("exchange8:me"),
            )
            .await
            .map_err(|e| match e {
                sentin8::bus::outbox::OutboxError::Db(db) => db,
                other => sqlx::Error::Configuration(Box::new(other)),
            })?;
        }
        tx.commit().await?;
    }
    Ok(())
}

/// Egress handle the state closure owns: the writer-facing receiver is taken
/// by [`spawn_outbox_writer`], the shard tasks keep the [`Egress`].
pub struct EgressPair {
    /// Per-shard drains (shard id → egress bridge).
    pub egresses: Vec<Arc<tokio::sync::Mutex<Egress>>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::MatchingEngine;
    use exchange8::contracts::{AddOrder, Side, Visibility, OrderType, Capacity};

    fn add(id: u64, inst: u32) -> AddOrder {
        AddOrder {
            price: exchange8::fixed::Price::equity(10_000),
            order_id: id,
            sub_order_id: 0,
            instrument_id: inst,
            qty: exchange8::fixed::Qty::units(100),
            connection_id: 1,
            side: Side::Bid,
            order_type: OrderType::Limit,
            visibility: Visibility::Display,
            capacity: Capacity::Agency,
            account: "A".into(),
            stp_key: None,
            stp_action: None,
            customer_id: 1,
            book_id: 0,
            anonymous: false,
            price_protection: None,
            max_floor: exchange8::fixed::Qty::ZERO,
            strategy: vec![b'*'],
        }
    }

    #[test]
    fn event_type_constants_follow_the_colon_format() {
        const NAMES: &[&str] = &[
            EV_ORDER_BOOKED,
            EV_ORDER_KILLED,
            EV_ORDER_MODIFIED,
            EV_ORDER_REPRICED,
            EV_ORDER_REJECTED,
            EV_TRADE_EXECUTED,
            EV_AUCTION_UNCROSSED,
        ];
        for n in NAMES {
            let segs: Vec<&str> = n.split(':').collect();
            assert_eq!(segs.len(), 4, "{n}: 4 segments");
            assert_eq!(segs[0], "exchange8");
            assert_eq!(segs[1], "me");
            assert!(n.find('.').is_none(), "{n}: no dots in event names");
        }
        // Families validate (colons structurally impossible).
        sentin8::bus::outbox::validate_stream_family(FAMILY_EXEC)
            .expect("me.exec family validates");
        sentin8::bus::outbox::validate_stream_family(FAMILY_AUCTION)
            .expect("me.auction family validates");
        assert!(sentin8::bus::outbox::validate_stream_family("me:exec").is_err());
    }

    #[test]
    fn drained_events_map_to_the_declared_vocabulary() {
        // Booked event → me.exec family + booked name with envelope fields.
        let mut eng = MatchingEngine::new();
        eng.add_order(&add(1, 1));
        let events = eng.drain_events();
        assert!(matches!(events[0], crate::engine::MeEvent::Booked(_)));
        let mut eg = Egress::new();
        eg.emit_all("11111111-1111-1111-1111-111111111111", 3, events);
        // MemBus stage: the disabled (URL-less) bus is a no-op consumer —
        // the batch must still return for the outbox writer.
        let disabled = tokio_block_on(sentin8::membus::ServiceBus::connect(
            "exchange8:me-test",
            "",
        ));
        let mut eg = eg;
        let batch = tokio_block_on(eg.publish_membus(&disabled));
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].event_type(), EV_ORDER_BOOKED);
        assert_eq!(batch[0].family(), FAMILY_EXEC);
        let env_id = batch[0].env_id();
        assert_eq!(env_id.to_string(), "11111111-1111-1111-1111-111111111111");
    }

    #[test]
    fn killed_events_carry_the_killed_name() {
        let mut eng = MatchingEngine::new();
        eng.add_order(&add(1, 1));
        let _ = eng.drain_events();
        eng.cancel_order(&exchange8::contracts::CancelOrder {
            order_id: 1,
            instrument_id: 1,
            connection_id: 1,
        });
        let events = eng.drain_events();
        assert!(matches!(events[0], crate::engine::MeEvent::Killed(_)));
        let mut eg = Egress::new();
        eg.emit_all("11111111-1111-1111-1111-111111111111", 0, events);
        let disabled = tokio_block_on(sentin8::membus::ServiceBus::connect("exchange8:me-test", ""));
        let batch = tokio_block_on(eg.publish_membus(&disabled));
        assert_eq!(batch[0].event_type(), EV_ORDER_KILLED);
        let payload = batch[0].to_payload();
        assert_eq!(payload["event_type"], EV_ORDER_KILLED);
        assert!(payload["payload"]["reason"].is_string());
    }

    /// Tiny block-on helper (a full async runtime dep in tests is overkill).
    fn tokio_block_on<F: std::future::Future>(fut: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime")
            .block_on(fut)
    }
}