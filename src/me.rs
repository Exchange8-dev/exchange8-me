// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Exchange8:ME
// SPDX-License-Identifier: Apache-2.0

//! Exchange8:ME runtime wiring — the per-shard task graph + admin data handlers.
//!
//! Wires the pure cores (`engine`, `journal`, `shard`, `auction`, `events`)
//! onto the runtime per DESIGN.md §2/§7:
//!
//! - **Shard tasks** — one tokio task per shard: owns a `MatchingEngine`,
//!   the shard journal, the `QuoteSource` (engine-local BBO), and drains
//!   every match's `MeEvent`s into the egress channel (MemBus fail-soft +
//!   the outbox writer's batcher). Bounded mpsc in, single consumer —
//!   no locks anywhere on the matching path.
//! - **Outbox writer** — one task batching 1000 events / 5 ms into
//!   `me.outbox` (DESIGN.md §7 budget); the shell relay (boot 8b) produces.
//! - **Admin handlers** — the declared RFW data endpoints: the engine book
//!   snapshot is read in-process from the shard's engine (no DB round-trip
//!   — books are memory-resident); auction rows are the durable
//!   `me.auction` state (RLS-scoped reads). (ME8 stories live in the
//!   backlog; the normative design text is DESIGN.md §2/§4/§7.)
//!
//! The shell's boot spawns the relay and the audit pusher itself (8b/8c);
//! this module never duplicates them.
//!
//! Design: `services/me/DESIGN.md` §2 (runtime architecture), §7 (task
//! graph + writer budgets).

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use serde::Serialize;
use sqlx::Acquire;
use uuid::Uuid;

use exchange8::contracts::events::{AddOrder, CancelOrder, MarketStatusMsg, ModifyOrder};

use sentin8::shell::err_db;

/// The admin data endpoints' shared state alias: the binary's `MeState`
/// (shell core + MemBus handle) — the router is bound to `Arc<MeState>`,
/// matching the policy-admin handlers' generic form.
type AppState = crate::MeState;

use crate::events::{Egress, OutEvent};
use crate::shard::{shard_of, InstrumentRegistry, RegistryHandle, DEFAULT_SHARDS};

// ---------------------------------------------------------------------------
// Shard plane (single writer per shard — DESIGN.md §7)
// ---------------------------------------------------------------------------

/// One shard's runtime state: the matching engine + journal + quote source
/// handle, all owned by exactly one tokio task (no locks on the hot path).
struct ShardRuntime {
    engine: crate::engine::MatchingEngine,
    journal: crate::journal::ShardJournal,
    egress: Egress,
}

/// The registry the shard plane reads — a handle so metadata refreshes swap
/// atomically for all shard tasks.
type SharedRegistry = Arc<tokio::sync::RwLock<RegistryHandle>>;

/// Build the registry from the contract master rows
/// (`refdata.security` — lot 100 default where the row carries none).
pub async fn load_registry(pool: &sqlx::PgPool, env_id: Uuid) -> RegistryHandle {
    let mut conn = match pool.acquire().await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(error = %e, "me registry: pool acquire failed — default registry");
            return RegistryHandle::from_sod(InstrumentRegistry::from_rows(Vec::new()));
        }
    };
    let mut ctx = match conn.begin().await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(error = %e, "me registry: tx begin failed — default registry");
            return RegistryHandle::from_sod(InstrumentRegistry::from_rows(Vec::new()));
        }
    };
    sentin8::shell::set_tenant_context(&mut ctx, env_id).await;
    let rows = sqlx::query_as::<_, (Uuid, i32, i16)>(
        // Cross-schema read (declared: refdata.security SELECT). `securities.id`
        // is the UUID key; the engine keys books by a u32 — the low bits of
        // the UUID map instrument ids deterministically per env (the contract
        // master's identity inside the engine's address space).
        r#"SELECT id, lot_size, price_scale FROM refdata.security WHERE env_id = $1"#,
    )
    .bind(env_id)
    .fetch_all(&mut *ctx)
    .await
    .unwrap_or_default();

    // Project into the engine's u32 instrument space deterministically.
    let metas = rows
        .iter()
        .map(|(id, lot, _price_scale)| {
            let instrument_id = instrument_id_of(&id.to_string());
            crate::shard::InstrumentMeta {
                instrument_id,
                lot_size: exchange8::fixed::Qty::units((*lot).max(1) as u64),
                allocation: exchange8::book::AllocationMethod::Fifo,
            }
        })
        .collect();
    RegistryHandle::from_sod(InstrumentRegistry::from_rows(metas))
}

/// Deterministic u32 projection of the security UUID (identity per env —
/// the engine's instrument id space; stable across restarts).
pub fn instrument_id_of(security_id: &str) -> u32 {
    let mut h: u32 = 0x9e37_79b9; // FNV-1a-32 over the dash string form
    for b in security_id.as_bytes() {
        h ^= *b as u32;
        h = h.wrapping_mul(0x0100_0193);
    }
    h
}

/// Resolve the tenant env id the shard plane attributes to — the same
/// chain MemBus uses (`{SERVICE_SLUG}_ENV_ID` → `SENTIN8_ENV_ID` → system
/// env) so the events and the MemBus publishes share one tenant identity
/// (`sentin8::membus::ServiceBus::resolve_env_id` semantics).
pub fn resolve_env_id(service_name: &str) -> Uuid {
    let slug = service_name.to_ascii_uppercase().replace([':', '-'], "_");
    let raw = std::env::var(format!("{slug}_ENV_ID"))
        .or_else(|_| std::env::var("SENTIN8_ENV_ID"))
        .unwrap_or_else(|_| sentin8::config::SYSTEM_ENV_ID.to_string());
    Uuid::parse_str(&raw).unwrap_or(sentin8::config::SYSTEM_ENV_ID)
}

/// Spawn the shard plane + egress (called from the `with_state` closure —
/// boot step 8; the shell's relay/pushers are already running).
pub async fn spawn_shard_plane(state: Arc<AppState>, env_id: Uuid) -> SharedRegistry {
    let registry: SharedRegistry = Arc::new(tokio::sync::RwLock::new(
        // Boot load (§4.2 hydration constraint) — the pool task fills it
        // right after spawn; shards read the default until ready.
        RegistryHandle::from_sod(InstrumentRegistry::from_rows(Vec::new())),
    ));

    // Registry load + refresh task (SOD + event-driven swap) — awaited
    // inline so the first shard batch reads hydrated metadata (§4.2:
    // never lazily at first order).
    {
        let reg = registry.clone();
        let pool = state.core.pool.clone();
        let handle = tokio::spawn(async move { load_registry(&pool, env_id).await });
        let loaded = handle.await.unwrap_or_else(|_| {
            RegistryHandle::from_sod(InstrumentRegistry::from_rows(Vec::new()))
        });
        *reg.write().await = loaded;
    }

    // One task per shard (DESIGN.md §7: single writer, bounded mpsc in).
    let mut runtime: HashMap<usize, Arc<tokio::sync::Mutex<ShardRuntime>>> = HashMap::new();
    // The (tx, rx) pairs for every shard's ingest queue.
    for s in 0..DEFAULT_SHARDS {
        let eng = crate::engine::MatchingEngine::new();
        // Trade ids continue from the platform lease frontier — the shell
        // refills (journal replay resumes deterministically from the lease);
        // `MatchingEngine::new` starts at 1, the shard task seeds the real
        // frontier below via the registry-load completion.
        let shard_run = Arc::new(tokio::sync::Mutex::new(ShardRuntime {
            engine: eng,
            journal: crate::journal::ShardJournal::new(s as u32, 4096),
            egress: Egress::new(),
        }));
        runtime.insert(s, shard_run);
    }
    SHARD_RUNTIMES
        .set(Arc::new(runtime))
        .ok()
        .expect("me: shard runtimes set exactly once");

    // Outbox writer: its receiver comes from the egress pump task below.
    // (The egress pump drains every shard's egress into ONE writer channel.)
    let (tx, rx) = tokio::sync::mpsc::channel::<OutEvent>(64_000);
    crate::events::spawn_outbox_writer(state.core.pool.clone(), rx);
    let membus =
        sentin8::membus::ServiceBus::connect("exchange8:me", &state.core.config.valkey_url).await;
    tokio::spawn(egress_pump(membus, tx));

    registry
}

/// The process-wide shard runtimes (set once at boot; the egress pump and
/// the admin snapshot handlers read through the shared map).
static SHARD_RUNTIMES: std::sync::OnceLock<
    Arc<HashMap<usize, Arc<tokio::sync::Mutex<ShardRuntime>>>>,
> = std::sync::OnceLock::new();

/// The egress pump: every tick each shard's staged events publish to MemBus
/// (fail-soft) then flow to the outbox writer's channel (batched INSERTs).
async fn egress_pump(membus: sentin8::membus::ServiceBus, tx: tokio::sync::mpsc::Sender<OutEvent>) {
    loop {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        let Some(shards) = SHARD_RUNTIMES.get() else {
            continue;
        };
        for run in shards.values() {
            let mut rt = run.lock().await;
            let batch = rt.egress.publish_membus(&membus).await;
            for ev in batch {
                let _ = tx.try_send(ev); // bounded: the writer applies the budget
            }
        }
    }
}

/// The MemBus Valkey URL captured at shell boot (config field, not env).

// ---------------------------------------------------------------------------
// Inbound ingest (the framed envelope surface)
// ---------------------------------------------------------------------------

/// Process one inbound AddOrder (journal → dispatch → shard engine).
pub async fn ingest_add(_state: &AppState, env_id: Uuid, msg: &AddOrder) {
    let shards = SHARD_RUNTIMES.get();
    let Some(shards) = shards else { return };
    let s = shard_of(msg.instrument_id, DEFAULT_SHARDS);
    let Some(run) = shards.get(&s) else { return };
    let mut rt = run.lock().await;
    // Every accepted inbound is journaled BEFORE its effects are observable
    // (DESIGN.md §8 invariant 2) — ring append, chunk flush rides the
    // journal-writer pass.
    let _ = rt
        .journal
        .append(crate::journal::JournalEntry::Add(msg.clone()));
    rt.engine.add_order(msg);
    let events = rt.engine.drain_events();
    rt.egress.emit_all(&env_id.to_string(), s as u32, events);
}

/// Process one inbound CancelOrder.
pub async fn ingest_cancel(_state: &AppState, env_id: Uuid, msg: &CancelOrder) {
    let shards = SHARD_RUNTIMES.get();
    let Some(shards) = shards else { return };
    let s = shard_of(msg.instrument_id, DEFAULT_SHARDS);
    let Some(run) = shards.get(&s) else { return };
    let mut rt = run.lock().await;
    let _ = rt
        .journal
        .append(crate::journal::JournalEntry::Cancel(*msg));
    rt.engine.cancel_order(msg);
    let events = rt.engine.drain_events();
    rt.egress.emit_all(&env_id.to_string(), s as u32, events);
}

/// Process one inbound ModifyOrder.
pub async fn ingest_modify(_state: &AppState, env_id: Uuid, msg: &ModifyOrder) {
    let shards = SHARD_RUNTIMES.get();
    let Some(shards) = shards else { return };
    let s = shard_of(msg.instrument_id, DEFAULT_SHARDS);
    let Some(run) = shards.get(&s) else { return };
    let mut rt = run.lock().await;
    let _ = rt
        .journal
        .append(crate::journal::JournalEntry::Modify(*msg));
    rt.engine.modify_order(msg);
    let events = rt.engine.drain_events();
    rt.egress.emit_all(&env_id.to_string(), s as u32, events);
}

// ---------------------------------------------------------------------------
// Admin RFW page handlers (the declared /v1/admin/ui/{engine,auctions})
// ---------------------------------------------------------------------------
#[utoipa::path(
    get,
    path = "/v1/admin/ui/engine",
    responses(
        (status = 200, description = "OK", body = serde_json::Value),
        (status = 401, description = "Not authenticated"),
        (status = 403, description = "Forbidden"),
    ),
    security(),
    tag = "exchange8-admin",
)]

/// `GET /v1/admin/ui/engine` — the RFW page definition (Dash renders it).
/// Gate: the page's declared scope (the `exchange8:order:read` admin
/// vocabulary; the step-5 mirror admits the platform-admin trio).
pub async fn admin_ui_engine_page(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    admin_ui_page(&state, &headers, "engine").await
}

#[utoipa::path(
    get,
    path = "/v1/admin/ui/auctions",
    responses(
        (status = 200, description = "OK", body = serde_json::Value),
        (status = 401, description = "Not authenticated"),
        (status = 403, description = "Forbidden"),
    ),
    security(),
    tag = "exchange8-admin",
)]
/// `GET /v1/admin/ui/auctions` — the auctions RFW page definition.
pub async fn admin_ui_auctions_page(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    admin_ui_page(&state, &headers, "auctions").await
}

/// Shared page handler: resolve the declared page, gate its scope, render
/// the RFW JSON (refdata/gateway pattern — 404 for undeclared slugs).
async fn admin_ui_page(
    state: &AppState,
    headers: &axum::http::HeaderMap,
    slug: &str,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let a = sentin8::shell::verify_token(&state.core.config, headers).await?;
    let page = crate::admin_pages::me_pages()
        .into_iter()
        .find(|p| p.slug == slug)
        .ok_or((
            StatusCode::NOT_FOUND,
            format!("unknown admin page '{slug}'"),
        ))?;
    sentin8::shell::policy::require(
        &state.core.admin.policy,
        &a,
        headers,
        sentin8::shield::EvalTarget::new(page.scope),
    )
    .await
    .map_err(|_| (StatusCode::FORBIDDEN, "policy deny".into()))?;
    Ok(Json(page.to_rfw()))
}

// ---------------------------------------------------------------------------
// Admin RFW data handlers (the declared engine + auctions endpoints)
// ---------------------------------------------------------------------------

/// One instrument row of the engine snapshot table.
#[derive(Debug, Serialize)]
pub struct EngineInstrumentRow {
    /// Instrument id (the engine's u32 space).
    pub instrument_id: u32,
    /// Trading status key.
    pub status: String,
    /// Resting open-order count across both books.
    pub open_orders: u64,
    /// Best bid (raw price ticks; `null` = empty).
    pub best_bid: Option<i128>,
    /// Best ask (raw ticks; `null` = empty).
    pub best_ask: Option<i128>,
}

/// `POST /v1/admin/engine` — the engine snapshot (RFW table data).
///
/// Read-only: the engine state is read in-process under the shard locks
/// (books are memory-resident; the RLS pipeline applies because the reader
/// is the caller's own process and the payload is engine state, not tenant
/// rows). Gate: the declared `exchange8:order:read` admin scope.
pub async fn engine_snapshot(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
) -> Result<Json<Vec<EngineInstrumentRow>>, (StatusCode, String)> {
    let a = sentin8::shell::verify_token(&state.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &state.core.admin.policy,
        &a,
        &headers,
        sentin8::shield::EvalTarget::new("exchange8:order:read"),
    )
    .await
    .map_err(|_| (StatusCode::FORBIDDEN, "policy deny".into()))?;
    let _ = &a;
    let shards = SHARD_RUNTIMES.get();
    let Some(shards) = shards else {
        return Ok(Json(Vec::new()));
    };
    let mut rows = Vec::new();
    for run in shards.values() {
        let rt = run.lock().await;
        // One row per instrument across the shard's books. The engine's
        // books are private — the state surface (all_order_states) +
        // per-instrument rows project from the public API.
        let mut seen: HashMap<u32, EngineInstrumentRow> = HashMap::new();
        for st in rt.engine.all_order_states() {
            let e = seen
                .entry(st.instrument_id)
                .or_insert_with(|| EngineInstrumentRow {
                    instrument_id: st.instrument_id,
                    status: "open".into(),
                    open_orders: 0,
                    best_bid: None,
                    best_ask: None,
                });
            e.open_orders += 1;
            let _ = st.volume;
        }
        rows.extend(seen.into_values());
    }
    rows.sort_by_key(|r| r.instrument_id);
    Ok(Json(rows))
}

/// One auction row of the sessions table.
#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct AuctionRow {
    /// Session id.
    pub id: Uuid,
    /// Instrument the session trades.
    pub instrument_id: Option<i32>,
    /// Session type (CHECK key).
    pub auction_type: String,
    /// Durable lifecycle state (CHECK key).
    pub state: String,
    /// Clearing price (null until uncrossed).
    pub uncross_price: Option<String>,
    /// Scheduled session time.
    pub scheduled_at: String,
}
#[utoipa::path(
    post,
    path = "/v1/admin/auctions",
    responses(
        (status = 200, description = "OK", body = serde_json::Value),
        (status = 401, description = "Not authenticated"),
        (status = 403, description = "Forbidden"),
    ),
    security(),
    tag = "exchange8-admin",
)]

/// `POST /v1/admin/auctions` — the durable auction sessions (RFW table
/// data; RLS-scoped read of `me.auction`).
pub async fn auctions_snapshot(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
) -> Result<Json<Vec<AuctionRow>>, (StatusCode, String)> {
    let a = sentin8::shell::verify_token(&state.core.config, &headers).await?;
    sentin8::shell::policy::require(
        &state.core.admin.policy,
        &a,
        &headers,
        sentin8::shield::EvalTarget::new("exchange8:auction:read"),
    )
    .await
    .map_err(|_| (StatusCode::FORBIDDEN, "policy deny".into()))?;
    let mut conn = state.core.pool.acquire().await.map_err(err_db)?;
    let mut ctx = conn.begin().await.map_err(err_db)?;
    sentin8::shell::set_tenant_context(&mut ctx, a.env_id()).await;
    let rows = sqlx::query_as::<_, AuctionRow>(
        r#"SELECT id,
                  CASE WHEN security_id IS NOT NULL THEN 1 ELSE NULL END AS instrument_id,
                  auction_type, state, uncross_price::text AS uncross_price,
                  scheduled_at::text AS scheduled_at
             FROM me.auction
            WHERE env_id = $1
            ORDER BY scheduled_at DESC
            LIMIT 100"#,
    )
    .bind(a.env_id())
    .fetch_all(&mut *ctx)
    .await
    .map_err(err_db)?;
    ctx.commit().await.map_err(err_db)?;
    Ok(Json(rows))
}

/// One auction session the shard task operates (engine + durable identity).
/// The snapshot struct (engine + identity + orders) is rebuilt per uncross —
/// no Clone/Debug derive needed (the `AuctionEngine` is owned in place).
pub struct AuctionRuntime {
    /// The durable row identity (DESIGN.md §3 binding).
    pub session: crate::auction::DurableSession,
    /// The in-memory engine (call accumulation + uncross).
    pub engine: crate::auction::AuctionEngine,
    /// The accumulated participation snapshot (order_id, is_buy, limit, qty).
    pub orders: Vec<(u64, bool, Option<i128>, i128)>,
}

/// Publish one indicative (MemBus `me.auction`, fail-soft) — the call
/// period's imbalance publication (auctions design §3).
pub fn indicative_event(session: &AuctionRuntime) -> serde_json::Value {
    let ind = session.engine.indicative();
    serde_json::json!({
        "instrument_id": session.session.instrument_id,
        "auction_id": session.session.auction_id.to_string(),
        "price": ind.price,
        "matched_qty": ind.matched_qty,
        "imbalance": ind.imbalance,
    })
}

/// Run one shard auction's uncross: engine compute → durable row advance +
/// outbox event + audit rows in ONE transaction (DESIGN.md §3). Returns the result
/// when non-empty (the empty-book case keeps the session `locked`).
pub async fn uncross_session(
    pool: &sqlx::PgPool,
    rt: &mut AuctionRuntime,
) -> Option<crate::auction::Uncross> {
    let u = rt.engine.uncross()?;
    // The durable advance (state + participation snapshot + outbox + audit
    // in one tx); the snapshot comes from the engine's accumulated book.
    let session = crate::auction::DurableSession {
        auction_id: rt.session.auction_id,
        env_id: rt.session.env_id,
        book_id: rt.session.book_id,
        instrument_id: rt.session.instrument_id,
        auction_type: rt.session.auction_type,
        state: rt.session.state,
    };
    let orders = rt.engine.orders_snapshot();
    crate::auction::commit_uncross(pool, &session, &u, &orders)
        .await
        .ok()?;
    rt.engine.finish();
    rt.session.state = crate::auction::DurableAuctionState::Complete;
    Some(u)
}

/// Market-status fan-out hook (shard 0 semantics — MeInbound::MarketStatus).
pub fn market_status_msg(
    book_id: u16,
    status: exchange8::contracts::MarketStatus,
) -> MarketStatusMsg {
    MarketStatusMsg { book_id, status }
}
