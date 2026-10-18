// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Exchange8:ME
// SPDX-License-Identifier: Apache-2.0

//! Service self-registration data for Exchange8:ME.
//!
//! Output via `--print-registration` (see the platform
//! `sentin8-service-self-registration-design.md`).

use utoipa::OpenApi;

use sentin8::registration::{
    AdminScope, AdminUi, AdminUiPage, CrossSchemaGrant, ErrorCode, M2mGrant, PgRole, PolicySetDecl,
    Registration, RelationDecl, SqlMigration,
};

/// The Shield text of the declared scope-mirror set — public so the
/// policy wiring tests assert against the REAL seed.
pub const SCOPE_MIRROR_POLICIES: &str = "permit(principal, action == Action::\"exchange8:me:manage\", resource) when { principal.scopes.contains(\"exchange8:me:manage\") };
permit(principal, action == Action::\"exchange8:me:manage\", resource) when { principal.scopes.contains(\"sentin8:sys:admin\") };
permit(principal, action == Action::\"exchange8:me:manage\", resource) when { principal.scopes.contains(\"sentin8:sys:org:admin\") };
permit(principal, action == Action::\"exchange8:me:manage\", resource) when { principal.scopes.contains(\"sentin8:sys:env:admin\") };";

/// Default declarative policy set (Sentin8:Policy design §3.2/§3.4,
/// seeded by `vertical enroll` as `origin='platform'`, `mode='enforced'`):
/// action-granular scope permits over the LIVE effective scope set — the
/// exact mirror of the gates this service's handlers evaluate in step 5
/// (policy design §4.3). Enforced: step 5 is the
/// only gate; shadow is the emergency one-UPDATE rollback lever only.
const POLICIES: &[PolicySetDecl] = &[PolicySetDecl {
    policy_set_name: "scope-mirror",
    description: "Action-granular scope permits for exchange8:me over the LIVE effective scope set. Enforced — step 5 is the only gate.",
    // Shield text lives in SCOPE_MIRROR_POLICIES (single source).
    policies: SCOPE_MIRROR_POLICIES,
    schema: None,
    mode: "enforced",
    default_enabled: true,
}];

/// Default ReBAC relation tuples (design §3.1c) — none declared by this
/// service today; env admins author their own graph via the relations
/// admin API.
const RELATIONS: &[RelationDecl] = &[];

const PG_ROLES: &[PgRole] = &[PgRole {
    scope: "exchange8:order:read,exchange8:order:write",
    role: "exchange8_me_service",
    bypass_rls: false,
    // Grants live in the exchange8 vertical database only (principles §6.6
    // database-per-vertical, schema-per-service).
    database: "exchange8",
    // Declared intra-database cross-schema reads (exactly these — never
    // blanket schema grants, principles §6.6a):
    cross_schema_grants: &[CrossSchemaGrant {
        schema: "refdata",
        table: "security",
        privilege: "SELECT",
    }],
}];

const ADMIN_SCOPES: &[AdminScope] = &[
    AdminScope::Typed {
        role: "exchange8:me:admin",
        scope: "exchange8:order:read",
    },
    AdminScope::Typed {
        role: "exchange8:me:admin",
        scope: "exchange8:order:write",
    },
    AdminScope::Typed {
        role: "exchange8:me:admin",
        scope: "exchange8:auction:read",
    },
    AdminScope::Typed {
        role: "exchange8:me:admin",
        scope: "exchange8:auction:write",
    },
    // Policy-admin surface (policy design §3.6; principles §6.0b per-service scope).
    AdminScope::Typed {
        role: "exchange8:me:admin",
        scope: "exchange8:me:policy:read",
    },
    AdminScope::Typed {
        role: "exchange8:me:admin",
        scope: "exchange8:me:policy:write",
    },
];

const ERROR_CODES: &[ErrorCode] = &[
    ErrorCode {
        enum_name: "Exchange8InstrumentHalted",
        error_code: "exchange8_instrument_halted",
        status_code: 409,
    },
    ErrorCode {
        enum_name: "Exchange8QuotationOnly",
        error_code: "exchange8_quotation_only",
        status_code: 409,
    },
    ErrorCode {
        enum_name: "Exchange8PriceProtection",
        error_code: "exchange8_price_protection",
        status_code: 409,
    },
];

const ADMIN_UI_PAGES: &[AdminUiPage] = &[
    AdminUiPage {
        route: "engine",
        label: "Engine",
        ui_endpoint: "/v1/admin/ui/engine",
        data_endpoint: "/v1/admin/engine",
        icon: "0xe8b5",
    },
    AdminUiPage {
        route: "auctions",
        label: "Auctions",
        ui_endpoint: "/v1/admin/ui/auctions",
        data_endpoint: "/v1/admin/auctions",
        icon: "0xe905",
    },
];

/// Workload scope grants (self-registration design §3.11). One entry per
/// workload this service presents a certificate for; `vertical enroll` upserts the
/// rows so the fail-closed registry map admits the registration. Scopes use the
/// full vocabulary form:
/// - `exchange8:me` / `me` — the service's own client (both
///   spellings granted; runtime call sites use the bare short name, the
///   map looks up the exact workload): bus produce/consume + the
///   shared certificate identity.
/// - `exchange8:me-policy` / `me-policy` — Shield hot-reload
///   consumer + stat emitter pair.
/// - `me-audit` — shell audit-journal pusher workload (§6.8; boot 8c).
/// - `exchange8-me-tls` — database-free HostContextResolver resolve workload
///   (TLS-RLS-4: `GET /v1/resolve/hostname`, scope
///   `sentin8:sys:instances:read`).
const M2M_GRANTS: &[M2mGrant] = &[
    M2mGrant {
        workload: "exchange8:me",
        scopes: &[
            "exchange8:order:read",
            "exchange8:order:write",
            "sentin8:bus:produce",
            "sentin8:bus:consume",
            "sentin8:stat:measure",
        ],
    },
    M2mGrant {
        workload: "me",
        scopes: &[
            "exchange8:order:read",
            "exchange8:order:write",
            "sentin8:bus:produce",
            "sentin8:bus:consume",
            "sentin8:stat:measure",
        ],
    },
    M2mGrant {
        workload: "exchange8:me-policy",
        scopes: &["sentin8:bus:consume", "sentin8:stat:measure"],
    },
    M2mGrant {
        workload: "me-policy",
        scopes: &["sentin8:stat:measure"],
    },
    M2mGrant {
        workload: "me-audit",
        scopes: &["sentin8:sys:audit:write"],
    },
    M2mGrant {
        workload: "exchange8-me-tls",
        scopes: &["sentin8:sys:instances:read"],
    },
];

const SQL_1_INIT: &str = include_str!("../sql/init.sql");

fn empty_openapi() -> serde_json::Value {
    serde_json::to_value(crate::openapi::MeOpenApi::openapi()).unwrap_or(serde_json::Value::Null)
}

pub fn print(section: Option<String>) -> anyhow::Result<()> {
    let reg = Registration {
        service_type: "exchange8:me",
        subdomain: "exchange8-me",
        health_path: "/v1/health",
        admin_scopes: ADMIN_SCOPES,
        error_codes: ERROR_CODES,
        consent_providers: &[],
        consent_flows: &[],
        admin_ui: Some(AdminUi {
            label: "Matching Engine",
            icon: "0xe8b5",
            service_flag: "exchange8-me",
            pages: ADMIN_UI_PAGES,
        }),
        sql_migrations: vec![SqlMigration {
            version: 1,
            name: "init",
            sql: SQL_1_INIT,
        }],
        pg_roles: PG_ROLES,
        audit_findings: None,
        service_actions: None,
        m2m_grants: Some(M2M_GRANTS),
        openapi_url: "/openapi.json",
        openapi: empty_openapi,
        policies: POLICIES,
        relations: RELATIONS,
        event_triggers: &[],
        schedules: &[],
    };
    sentin8::registration::print_registration(section, &reg, empty_openapi)
}
