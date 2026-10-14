// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Exchange8:ME
// SPDX-License-Identifier: Apache-2.0

//! Ex8:ME RFW admin page definitions (Dash renders; ME never ships UI code).
//!
//! Serves the pages the registration declares (`engine`, `auctions`) —
//! pure data following the sibling `services/refdata/src/admin_ui.rs`
//! `to_rfw` pattern: definitions are JSON, data endpoints alias the same
//! handlers (RFW renderer only supports POST for actions).
//!
//! Design: `services/me/DESIGN.md` §2 (admin lifecycle), §4 (API surface);
//! icons are Material Icons code points (hex), never names.

use serde_json::{json, Value};

/// One RFW page definition (the shape Dash renders).
#[derive(Clone, Debug, PartialEq)]
pub struct RfwPage {
    /// Route segment: served at /v1/admin/ui/{slug}.
    pub slug: &'static str,
    /// Page title.
    pub title: &'static str,
    /// Material Icons code point (hex string, not a name).
    pub icon_hex: &'static str,
    /// Required admin scope (the page's Shield action).
    pub scope: &'static str,
    /// Sections (RFW blocks: tables, actions).
    pub sections: Vec<RfwSection>,
}

/// One RFW section.
#[derive(Clone, Debug, PartialEq)]
pub enum RfwSection {
    /// Read-only table bound to a resource.
    Table {
        /// Section heading.
        heading: &'static str,
        /// The data resource the table renders (`/v1/admin/{resource}`).
        resource: &'static str,
        /// Columns (payload key, display label).
        columns: Vec<(&'static str, &'static str)>,
    },
    /// Action button bound to a POST route.
    Action {
        /// Section heading.
        heading: &'static str,
        /// The POST resource (the renderer only supports POST).
        resource: &'static str,
        /// Button label.
        label: &'static str,
        /// Inputs (payload key, display label).
        fields: Vec<(&'static str, &'static str)>,
    },
}

impl RfwPage {
    /// The RFW definition JSON served at `GET /v1/admin/ui/{slug}`.
    pub fn to_rfw(&self) -> Value {
        let sections: Vec<Value> = self
            .sections
            .iter()
            .map(|s| match s {
                RfwSection::Table {
                    heading,
                    resource,
                    columns,
                } => json!({
                    "type": "table",
                    "heading": heading,
                    "resource": resource,
                    "columns": columns.iter()
                        .map(|(k, l)| json!({"key": k, "label": l}))
                        .collect::<Vec<_>>(),
                }),
                RfwSection::Action {
                    heading,
                    resource,
                    label,
                    fields,
                } => json!({
                    "type": "action",
                    "heading": heading,
                    "resource": resource,
                    "label": label,
                    "fields": fields.iter()
                        .map(|(k, l)| json!({"key": k, "label": l}))
                        .collect::<Vec<_>>(),
                }),
            })
            .collect();
        json!({
            "slug": self.slug,
            "title": self.title,
            "icon": self.icon_hex,
            "scope": self.scope,
            "sections": sections,
        })
    }
}

/// The ME admin pages — the declared RFW set (`engine`, `auctions`).
pub fn me_pages() -> Vec<RfwPage> {
    vec![
        RfwPage {
            slug: "engine",
            title: "Matching Engine",
            icon_hex: "0xe8b5",
            scope: "exchange8:order:read",
            sections: vec![RfwSection::Table {
                heading: "Instrument books",
                resource: "engine",
                columns: vec![
                    ("instrument_id", "Instrument"),
                    ("halts", "Status"),
                    ("open_orders", "Open orders"),
                    ("best_bid", "Best bid"),
                    ("best_ask", "Best ask"),
                ],
            }],
        },
        RfwPage {
            slug: "auctions",
            title: "Auctions",
            icon_hex: "0xe905",
            scope: "exchange8:auction:read",
            sections: vec![
                RfwSection::Table {
                    heading: "Auction sessions",
                    resource: "auctions",
                    columns: vec![
                        ("id", "Session"),
                        ("instrument_id", "Instrument"),
                        ("auction_type", "Type"),
                        ("state", "State"),
                        ("uncross_price", "Uncross price"),
                    ],
                },
                RfwSection::Action {
                    heading: "Withdraw session",
                    resource: "auctions",
                    label: "Cancel",
                    fields: vec![("auction_id", "Auction ID")],
                },
            ],
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn declared_pages_render_and_match_registration() {
        let pages = me_pages();
        // The registration declares exactly these two routes.
        let slugs: Vec<&str> = pages.iter().map(|p| p.slug).collect();
        assert_eq!(slugs, vec!["engine", "auctions"]);
        for page in pages {
            let v = page.to_rfw();
            assert_eq!(v["slug"], page.slug);
            // Icons are Material code points — hex strings (the `0x` prefix
            // is the registration payload's spelling, accepted by Dash).
            let icon = v["icon"].as_str().unwrap();
            assert!(
                icon.trim_start_matches("0x")
                    .chars()
                    .all(|c| c.is_ascii_hexdigit()),
                "icon must be a Material code point (hex): {icon}"
            );
            assert!(v["scope"].as_str().unwrap().starts_with("exchange8:"));
            assert!(!v["sections"].as_array().unwrap().is_empty());
        }
    }

    #[test]
    fn action_resources_are_post_friendly() {
        // The RFW renderer only supports POST — actions never name PUT/DELETE.
        for page in me_pages() {
            for s in &page.sections {
                if let RfwSection::Action { resource, .. } = s {
                    assert!(!resource.contains("PUT"));
                    assert!(!resource.contains("DELETE"));
                }
            }
        }
    }
}
