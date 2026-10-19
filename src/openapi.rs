// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Exchange8:Me
// SPDX-License-Identifier: Apache-2.0

//! OpenAPI spec for Exchange8:Me.
//!
//! utoipa annotations on the service's handlers + the shared Shield
//! policy-admin surface (`sentin8::shell::policy_*` wrappers); the
//! shell-owned `/v1/health` mount is described through a spec wrapper
//! (`sentin8/design/sentin8-openapi-coverage-design.md` §2).

use utoipa::OpenApi;

pub mod w {

    /// Spec wrapper for the shell-owned `/v1/health` mount.
    #[utoipa::path(
        get,
        path = "/v1/health",
        responses(
            (status = 200, description = "Service is healthy", body = sentin8::middleware::health::LiveHealth),
        ),
        security(),
        tag = "me",
    )]
    pub async fn me_health() {}
}

/// OpenAPI spec for the me API.
#[derive(OpenApi)]
#[openapi(
    paths(
crate::me::admin_ui_engine_page,
crate::me::admin_ui_auctions_page,
crate::me::auctions_snapshot,
crate::me::engine_snapshot,
crate::openapi::w::me_health,
        sentin8::shell::policy_list_sets,
        sentin8::shell::policy_get_set,
        sentin8::shell::policy_put_set,
        sentin8::shell::policy_delete_set,
        sentin8::shell::policy_test_set,
        sentin8::shell::policy_mode_enforce,
        sentin8::shell::policy_mode_shadow,
        sentin8::shell::policy_mode_disable,
        sentin8::shell::policy_list_relations,
        sentin8::shell::policy_put_relation,
        sentin8::shell::policy_delete_relation,
        sentin8::shell::policy_check_relation,
        sentin8::shell::policy_list_attributes,
        sentin8::shell::policy_put_attribute,
        sentin8::shell::policy_delete_attribute,
    ),
    components(schemas(
        crate::me::AdminRfwPage,
        crate::me::AuctionRow,
        crate::me::EngineInstrumentRow,
        sentin8::middleware::health::LiveHealth,
    )),
    security(),
    tags(
        (name = "me", description = "Exchange8:Me"),
        (name = "exchange8-admin", description = "Exchange8 admin surface (RFW)"),
        (name = "policy", description = "Shield policy administration (shared surface)"),
    ),
)]
pub struct MeOpenApi;
