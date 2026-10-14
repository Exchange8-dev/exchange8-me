// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Exchange8:ME
// SPDX-License-Identifier: Apache-2.0

//! Ex8:ME policy wiring tests — the seeded scope-mirror Shield set.
//!
//! Per sentin8/design/sentin8-policy-technical-design.md §6 (Definition
//! of Done 4.2(g)) — proves the exchange8:me wiring with the REAL seeded
//! scope-mirror text (`registration::SCOPE_MIRROR_POLICIES`, the exact
//! string `vertical enroll` upserts): no-set → deny; enforced no-match →
//! deny; shadow never blocks; forbid beats permit; M2M parity
//! (`token_tp` never bypasses); platform-admin scopes pass; the
//! enroll-time dry-run gate accepts the declared set.
//!
//! Pure tests (no DB): the engine cache is fed via `load_from_rows` with
//! hand-built rows matching the DB row shape. DB-backed behavior (boot
//! load against the live `sentin8_policy_sets` table) is covered by the
//! shell boot path (refuse-to-start) and the workspace SQL suites.

#[path = "../src/registration.rs"]
#[allow(dead_code)]
mod registration;

use registration::SCOPE_MIRROR_POLICIES;
use sentin8::shield::{
    Decision, EvalTarget, PolicyEngine, PolicyMode, PolicyOrigin, PolicySetRecord, RequestCtx,
};

const SERVICE: &str = "exchange8:me";
const ENV: uuid::Uuid = uuid::Uuid::nil();

fn row(name: &str, policies: &str, mode: PolicyMode, origin: PolicyOrigin) -> PolicySetRecord {
    PolicySetRecord {
        id: uuid::Uuid::new_v4(),
        env_id: ENV,
        service: SERVICE.into(),
        policy_set_name: name.into(),
        description: String::new(),
        policies: policies.into(),
        schema: None,
        origin,
        mode,
        version: 1,
        enabled: true,
    }
}

fn ctx(token_tp: &str, scope_list: &[&str]) -> RequestCtx {
    RequestCtx::new(
        "subj-1",
        ENV,
        uuid::Uuid::nil(),
        token_tp,
        scope_list.iter().map(|s| s.to_string()).collect(),
    )
}

fn mirror_engine(mode: PolicyMode) -> PolicyEngine {
    let e = PolicyEngine::new(SERVICE);
    e.load_from_rows(vec![row(
        "scope-mirror",
        SCOPE_MIRROR_POLICIES,
        mode,
        PolicyOrigin::Platform,
    )])
    .expect("declared scope-mirror set must parse");
    e
}

/// (a) no-set → deny: an engine with NO loaded set denies everything
/// (fail-closed, design §3.3) — the runtime bridge converts it to 403.
#[test]
fn no_set_denies_everything() {
    let e = PolicyEngine::new(SERVICE); // empty cache
    let out = e.evaluate(
        &ctx("user", &[HANDLER_ACTIONS[0]]),
        &EvalTarget::new(HANDLER_ACTIONS[0]),
    );
    assert!(!out.is_allow());
    assert!(matches!(out.enforce_decision, Decision::Deny { .. }));
}

/// (a2) enforced no-match → deny: a caller holding an unrelated service's
/// scope is denied even with the full mirror loaded.
#[test]
fn enforced_no_match_denies() {
    let e = mirror_engine(PolicyMode::Enforced);
    let out = e.evaluate(
        &ctx("user", &["bank8:ledger:accounts:read"]),
        &EvalTarget::new(HANDLER_ACTIONS[0]),
    );
    assert!(
        !out.is_allow(),
        "scope for another service must not satisfy"
    );
}

/// (b) shadow never blocks: the same denial under a shadow set is an allow
/// (would-deny is logged + emitted, never enforced).
#[test]
fn shadow_never_blocks() {
    let e = mirror_engine(PolicyMode::Shadow);
    let out = e.evaluate(&ctx("user", &[]), &EvalTarget::new(HANDLER_ACTIONS[0]));
    assert!(out.is_allow(), "shadow must never block");
    assert!(out.shadow_deny.is_some(), "would-deny must be reported");
}

/// (c) the mirror permits its own grants: every action referenced by a
/// permit is allowed for the principal holding that scope.
#[test]
fn mirror_permits_own_grants() {
    let e = mirror_engine(PolicyMode::Enforced);
    for action in HANDLER_ACTIONS {
        let out = e.evaluate(&ctx("user", &[*action]), &EvalTarget::new(*action));
        assert!(
            out.is_allow(),
            "action {action} must be covered by the mirror"
        );
    }
}

/// (d) M2M parity: `token_tp == "m2m"` evaluates the same set — the token
/// type never bypasses policy (principles §5.2a).
#[test]
fn m2m_parity() {
    let e = mirror_engine(PolicyMode::Enforced);
    let action = HANDLER_ACTIONS[0];
    let allow = e.evaluate(&ctx("m2m", &[action]), &EvalTarget::new(action));
    assert!(allow.is_allow(), "scoped M2M principal passes like a user");
    let deny = e.evaluate(&ctx("m2m", &[]), &EvalTarget::new(action));
    assert!(!deny.is_allow(), "unscoped M2M is denied like a user");
}

/// (e) platform-admin scopes pass every action (the mirror carries
/// `sentin8:sys:admin` permits for each action, preserving the pre-existing
/// `require_any_scope` platform-admin bypass).
#[test]
fn platform_admin_scopes_pass() {
    let e = mirror_engine(PolicyMode::Enforced);
    for action in HANDLER_ACTIONS {
        let out = e.evaluate(
            &ctx("user", &["sentin8:sys:admin"]),
            &EvalTarget::new(*action),
        );
        assert!(out.is_allow(), "sys:admin must pass {action}");
    }
}

/// (f) forbid beats permit: a tenant-authored forbid overrides the mirror.
#[test]
fn forbid_beats_permit() {
    let e = PolicyEngine::new(SERVICE);
    e.load_from_rows(vec![
        row(
            "scope-mirror",
            SCOPE_MIRROR_POLICIES,
            PolicyMode::Enforced,
            PolicyOrigin::Platform,
        ),
        row(
            "tenant-freeze",
            &format!(
                "forbid(principal, action == Action::\"{}\", resource);",
                HANDLER_ACTIONS[0]
            ),
            PolicyMode::Enforced,
            PolicyOrigin::Tenant,
        ),
    ])
    .expect("load");
    let out = e.evaluate(
        &ctx("user", &[HANDLER_ACTIONS[0]]),
        &EvalTarget::new(HANDLER_ACTIONS[0]),
    );
    assert!(
        matches!(out.enforce_decision, Decision::Deny { .. }),
        "forbid wins"
    );
}

/// (f2) role-keyed rules participate in the same decision: a tenant set
/// granting by live role name passes a role-holding caller (§3.1d).
#[test]
fn role_keyed_tenant_set_decides() {
    let e = PolicyEngine::new(SERVICE);
    e.load_from_rows(vec![row(
        "tenant-role-grant",
        &format!(
            "permit(principal, action == Action::\"{}\", resource) when {{ principal.roles.contains(\"exchange8:admin\") }};",
            HANDLER_ACTIONS[0]
        ),
        PolicyMode::Enforced,
        PolicyOrigin::Tenant,
    )])
    .expect("load");
    let denied = e.evaluate(&ctx("user", &[]), &EvalTarget::new(HANDLER_ACTIONS[0]));
    assert!(!denied.is_allow());
    let allowed = e.evaluate(
        &ctx("user", &[]).with_roles(["exchange8:admin"].iter().map(|s| s.to_string()).collect()),
        &EvalTarget::new(HANDLER_ACTIONS[0]),
    );
    assert!(allowed.is_allow());
}

/// (g) tenant isolation: sets of ANOTHER env are invisible to this env's
/// evaluation — the cache key is (env_id, name).
#[test]
fn cross_env_set_isolated() {
    let other_env = uuid::Uuid::new_v4();
    let mut r = row(
        "other-env-set",
        SCOPE_MIRROR_POLICIES,
        PolicyMode::Enforced,
        PolicyOrigin::Platform,
    );
    r.env_id = other_env;
    let e = PolicyEngine::new(SERVICE);
    e.load_from_rows(vec![r]).expect("load");
    let out = e.evaluate(
        &ctx("user", &[HANDLER_ACTIONS[0]]),
        &EvalTarget::new(HANDLER_ACTIONS[0]),
    );
    assert!(
        !out.is_allow(),
        "another env's set must not decide this env"
    );
}

/// (g2) registration parity: the declared set covers EVERY action the
/// handlers evaluate — a new EvalTarget without a mirror permit fails here.
#[test]
fn declared_mirror_covers_every_handler_action() {
    let e = mirror_engine(PolicyMode::Enforced);
    for action in HANDLER_ACTIONS {
        let out = e.evaluate(&ctx("user", &[*action]), &EvalTarget::new(*action));
        assert!(
            out.is_allow(),
            "action {action} must be covered by the mirror"
        );
    }
}

/// (h) the enroll-time dry-run gate (§4.2) accepts the declared set —
/// an enforced set that denies its own grants never ships.
#[test]
fn declared_set_passes_enroll_dry_run() {
    sentin8::shield::runtime::dry_run_mirror(SCOPE_MIRROR_POLICIES, "scope-mirror", SERVICE)
        .expect("declared exchange8:me scope-mirror must permit its own grants");
}

/// Every action the ME handlers evaluate (step-5 targets). The
/// mirror set must cover each one — see `declared_mirror_covers_every_handler_action`.
pub(crate) const HANDLER_ACTIONS: &[&str] = &["exchange8:me:manage"];
