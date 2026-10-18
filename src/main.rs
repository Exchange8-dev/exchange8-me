// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Exchange8:ME
// SPDX-License-Identifier: Apache-2.0

//! Exchange8:ME service binary — instrument-sharded deterministic matching engine.
//!
//! Design: `design/sentin8-exchange8-matching-engine-technical-design.md`;
//! detailed design: `services/me/DESIGN.md` §2.
//! The core engine lives in `engine.rs` (single-writer per instrument)
//! and the auction uncrossing in `auction.rs`; this shell wires the tokio
//! HTTP/TLS surface via the platform `sentin8-shell` crate (registration,
//! DB pool, health).

#![allow(dead_code)]

// (master design §11.1). Workers (bus consumers, session
// supervisors, purge jobs) are in-process tokio tasks, so the
mod admin_pages;
mod auction;
mod engine;
mod events;
mod journal;
mod me;
mod registration;
mod openapi;
mod shard;

use std::sync::Arc;

/// Service state: shell core + the MemBus publish handle.
///
/// `ServiceBus` encodes the fail-soft policy once for every realtime
/// adopter: an empty `valkey_url` or an unreachable Valkey disables
/// publishing (WARN, never a startup failure) — the ME must still boot
/// (`sentin8/design/sentin8-membus-technical-design.md` §10.1.1).
pub struct MeState {
    pub core: sentin8::shell::ServiceCore,
    pub membus: sentin8::membus::ServiceBus,
}

fn extend_router(
    app: axum::Router<std::sync::Arc<MeState>>,
    _state: std::sync::Arc<MeState>,
) -> axum::Router<std::sync::Arc<MeState>> {
    // Order flow arrives via direct e8-client streams from Gateway/OMS/SOR
    // (design §4.1) — ME mounts NO inbound order HTTP surface. The router
    // carries health (shell), the Shield policy-admin API, and the declared
    // RFW admin pages + data endpoints.

    app
        // ── Sentin8:Policy admin API (shared implementation;
        // handlers generic over this state via the HasPolicyAdmin impl below;
        // axum 0.8 cannot merge a bound Router<()> into Router<std::sync::Arc<MeState>>).
        .route(
            "/v1/admin/policy-sets",
            axum::routing::get(sentin8::shield::admin::list_policy_sets::<std::sync::Arc<MeState>>),
        )
        .route(
            "/v1/admin/policy-sets/{name}",
            axum::routing::get(sentin8::shield::admin::get_policy_set::<std::sync::Arc<MeState>>)
                .put(sentin8::shield::admin::put_policy_set::<std::sync::Arc<MeState>>)
                .delete(sentin8::shield::admin::delete_policy_set::<std::sync::Arc<MeState>>),
        )
        .route(
            "/v1/admin/policy-sets/{name}/test",
            axum::routing::post(sentin8::shield::admin::test_policy_set::<std::sync::Arc<MeState>>),
        )
        .route(
            "/v1/admin/policy-sets/enforce",
            axum::routing::post(
                sentin8::shield::admin::admin_enforce_policy_set::<std::sync::Arc<MeState>>,
            ),
        )
        .route(
            "/v1/admin/policy-sets/shadow",
            axum::routing::post(
                sentin8::shield::admin::admin_shadow_policy_set::<std::sync::Arc<MeState>>,
            ),
        )
        .route(
            "/v1/admin/policy-sets/disable",
            axum::routing::post(
                sentin8::shield::admin::admin_disable_policy_set::<std::sync::Arc<MeState>>,
            ),
        )
        .route(
            "/v1/admin/relations",
            axum::routing::get(sentin8::shield::admin::list_relations::<std::sync::Arc<MeState>>)
                .put(sentin8::shield::admin::put_relation::<std::sync::Arc<MeState>>)
                .delete(sentin8::shield::admin::delete_relation::<std::sync::Arc<MeState>>),
        )
        .route(
            "/v1/admin/relations/check",
            axum::routing::get(sentin8::shield::admin::check_relation::<std::sync::Arc<MeState>>),
        )
        .route(
            "/v1/admin/attributes",
            axum::routing::get(sentin8::shield::admin::list_attributes::<std::sync::Arc<MeState>>)
                .put(sentin8::shield::admin::put_attribute::<std::sync::Arc<MeState>>)
                .delete(sentin8::shield::admin::delete_attribute::<std::sync::Arc<MeState>>),
        )
        // ── Declared RFW admin pages (registration admin_ui: `engine`,
        // `auctions` — DESIGN.md §4; definitions pure data in
        // admin_pages.rs, data endpoints in me.rs).
        .route(
            "/v1/admin/ui/engine",
            axum::routing::get(me::admin_ui_engine_page),
        )
        .route(
            "/v1/admin/ui/auctions",
            axum::routing::get(me::admin_ui_auctions_page),
        )
        .route(
            // RFW renderer only supports POST — the data endpoints are
            // POST-alias routes per the refdata/gateway pattern.
            "/v1/admin/engine",
            axum::routing::post(me::engine_snapshot),
        )
        .route(
            "/v1/admin/auctions",
            axum::routing::post(me::auctions_snapshot),
        )
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    sentin8::shell::serve(
        sentin8::shell::Sentin8Shell::new("exchange8:me", "EXCHANGE8_ME")
            .with_registration(registration::print)
            .with_state(|core| async move {
                // the opt-in line: ME publishes `me.exec`
                // (stream class) onto the platform Memory Bus.
                let membus =
                    sentin8::membus::ServiceBus::connect("exchange8:me", &core.config.valkey_url)
                        .await;
                // The shard plane + egress stack spawn at boot (DESIGN.md
                // §2 item 5): registry SOD load, one task per shard, the
                // egress pump (MemBus + outbox writer batching 1000 ev /
                // 5 ms). The shell's relay (8b) and audit pusher (8c) are
                // already running — they drain what ME stages.
                let state = Arc::new(MeState { core, membus });
                // The env id follows the same resolve chain MemBus uses
                // (`EXCHANGE8_ME_ENV_ID` → `SENTIN8_ENV_ID` → system env)
                // so the shard plane attributes to the same tenant the
                // shell publishes as.
                let env_id = me::resolve_env_id("exchange8:me");
                me::spawn_shard_plane(state.clone(), env_id).await;
                Ok(state)
            })
            .with_router(extend_router),
    )
    .await
}

/// Shared policy-admin surface accessor (shared Shield admin surface): the shell boot
/// loaded the engine into `core.admin`; the shared handlers extract it via
/// `HasPolicyAdmin` so the admin surface and the request path evaluate the
/// SAME cache. (MeState is bin-local, so the impl lives here.)
impl sentin8::shield::admin::HasPolicyAdmin for MeState {
    fn policy_admin(&self) -> &sentin8::shield::admin::SharedAdminState {
        &self.core.admin
    }
}
