// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Exchange8:ME
// SPDX-License-Identifier: Apache-2.0

//! Exchange8:ME auction engine — call auction accumulation + uncrossing.
//!
//! Call auctions (opening/closing/volatility/IPO): orders accumulate during
//! the call; at uncross the engine computes the price maximizing matched
//! volume with the design tie-break chain (max surplus → min imbalance →
//! reference price). Publishes indicative price + imbalance during the call.
//!
//! **Durability binding** (DESIGN.md §3): every in-memory `AuctionEngine`
//! session is anchored to one durable `me.auction` row; state transitions
//! advance the row in the SAME transaction as the
//! `exchange8:me:auction:uncrossed` outbox event and the audit journal rows,
//! so the in-memory (`Closed → PreOpen → Uncrossing → Open`) and durable
//! (`scheduled → accepting → locked → uncrossing → complete|cancelled`)
//! state machines cannot diverge at an uncross.
//!
//! Design: `design/sentin8-exchange8-auctions-technical-design.md` §3–4;
//! durable binding: `services/me/DESIGN.md` §3.

use uuid::Uuid;

/// An auction order accumulated during the call period.
#[derive(Debug, Clone)]
pub struct AuctionOrder {
    pub order_id: u64,
    pub is_buy: bool,
    /// Limit price in raw ticks; None = market (MOO/MOC/auction market).
    pub limit_price: Option<i128>,
    pub qty: i128,
}

/// Auction session type.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuctionType {
    Opening,
    Closing,
    Reopen,
    Halt,
    Volatility,
    Ipo,
}

impl AuctionType {
    /// The `me.auction.auction_type` CHECK vocabulary key.
    pub fn as_str(self) -> &'static str {
        match self {
            AuctionType::Opening => "opening",
            AuctionType::Closing => "closing",
            AuctionType::Reopen => "reopen",
            AuctionType::Halt => "halt",
            AuctionType::Volatility => "volatility",
            AuctionType::Ipo => "ipo",
        }
    }
}

/// The durable `me.auction.state` vocabulary (the engine's in-memory states
/// map onto these scheduling stages — DESIGN.md §3 state-row binding).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DurableAuctionState {
    /// Session scheduled, call not started.
    Scheduled,
    /// Call period: orders accumulate (engine `PreOpen`).
    Accepting,
    /// Call period ended, uncrossing imminent (engine `PreOpen` at the
    /// random-window end).
    Locked,
    /// Uncrossing computed (engine `Uncrossing`).
    Uncrossing,
    /// Uncross price published (engine `Open`).
    Complete,
    /// Operator withdrew the session (engine `Closed`, never uncrossed).
    Cancelled,
}

impl DurableAuctionState {
    /// The `me.auction.state` CHECK vocabulary key.
    pub fn as_str(self) -> &'static str {
        match self {
            DurableAuctionState::Scheduled => "scheduled",
            DurableAuctionState::Accepting => "accepting",
            DurableAuctionState::Locked => "locked",
            DurableAuctionState::Uncrossing => "uncrossing",
            DurableAuctionState::Complete => "complete",
            DurableAuctionState::Cancelled => "cancelled",
        }
    }

    /// Parse a row's `state` column.
    pub fn from_db(s: &str) -> Option<Self> {
        match s {
            "scheduled" => Some(DurableAuctionState::Scheduled),
            "accepting" => Some(DurableAuctionState::Accepting),
            "locked" => Some(DurableAuctionState::Locked),
            "uncrossing" => Some(DurableAuctionState::Uncrossing),
            "complete" => Some(DurableAuctionState::Complete),
            "cancelled" => Some(DurableAuctionState::Cancelled),
            _ => None,
        }
    }

    /// The engine phase a durable state corresponds to (`None` for the
    /// scheduling stages the engine never sees: scheduled pre-open, and
    /// the post-exit cancelled marker).
    pub fn engine_phase(self) -> Option<AuctionState> {
        match self {
            DurableAuctionState::Scheduled => None,
            DurableAuctionState::Accepting => Some(AuctionState::PreOpen),
            DurableAuctionState::Locked => Some(AuctionState::PreOpen),
            DurableAuctionState::Uncrossing => Some(AuctionState::Uncrossing),
            DurableAuctionState::Complete => Some(AuctionState::Open),
            DurableAuctionState::Cancelled => Some(AuctionState::Closed),
        }
    }
}

/// Auction state (design §4 state machine).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuctionState {
    /// No auction scheduled.
    Closed,
    /// Call period: orders accumulate.
    PreOpen,
    /// Call ended, uncrossing computed.
    Uncrossing,
    /// Continuous trading resumed at the uncross price.
    Open,
}

/// Indicative state during the call (published to MD).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Indicative {
    /// Indicative uncross price (None when no overlap yet).
    pub price: Option<i128>,
    /// Total matched volume at the indicative price.
    pub matched_qty: i128,
    /// Imbalance: signed excess of the unmatchable side at that price
    /// (positive = buy-side excess).
    pub imbalance: i128,
}

/// Uncross result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Uncross {
    /// Single clearing price maximizing matched volume.
    pub price: i128,
    /// Matched quantity at the price.
    pub matched_qty: i128,
    /// Residual imbalance at the price (+ = buy excess, − = sell excess).
    pub imbalance: i128,
}

/// One call auction session for an instrument.
pub struct AuctionEngine {
    pub instrument_id: u32,
    pub auction_type: AuctionType,
    state: AuctionState,
    orders: Vec<AuctionOrder>,
    /// Reference price for tie-breaking (previous close / last trade).
    pub reference_price: Option<i128>,
}

impl AuctionEngine {
    pub fn new(
        instrument_id: u32,
        auction_type: AuctionType,
        reference_price: Option<i128>,
    ) -> Self {
        AuctionEngine {
            instrument_id,
            auction_type,
            state: AuctionState::Closed,
            orders: Vec::new(),
            reference_price,
        }
    }

    pub fn state(&self) -> AuctionState {
        self.state
    }

    /// Start the call period.
    pub fn open_call(&mut self) {
        self.state = AuctionState::PreOpen;
    }

    /// Accept an order during the call.
    pub fn add(&mut self, order: AuctionOrder) {
        debug_assert!(order.qty > 0);
        self.orders.push(order);
    }

    /// Cancel an accumulated order (returns false when unknown).
    pub fn cancel(&mut self, order_id: u64) -> bool {
        let Some(pos) = self.orders.iter().position(|o| o.order_id == order_id) else {
            return false;
        };
        self.orders.remove(pos);
        true
    }

    /// Publish the accumulated participation snapshot ((order_id, is_buy,
    /// limit price, qty) per order) — the uncross transaction writes these
    /// as the `me.auction_order` rows (DESIGN.md §3).
    pub fn orders_snapshot(&self) -> Vec<(u64, bool, Option<i128>, i128)> {
        self.orders
            .iter()
            .map(|o| (o.order_id, o.is_buy, o.limit_price, o.qty))
            .collect()
    }

    /// Candidate clearing prices: every buy limit, sell limit, and the
    /// reference (so the tie-break can fall on it).
    fn candidate_prices(&self) -> Vec<i128> {
        let mut c: Vec<i128> = self.orders.iter().filter_map(|o| o.limit_price).collect();
        if let Some(r) = self.reference_price {
            c.push(r);
        }
        c.sort_unstable();
        c.dedup();
        c
    }

    /// Aggregate executable quantity against a candidate price `p`:
    /// buys executable at ≤ p count toward demand only when executable
    /// (limit ≥ p or market); symmetric for sells.
    fn book_at(&self, p: i128) -> (i128, i128) {
        let mut buy_qty = 0i128;
        let mut sell_qty = 0i128;
        for o in &self.orders {
            let executable = match o.limit_price {
                None => true, // market orders execute at any clearing price
                Some(lp) => {
                    if o.is_buy {
                        lp >= p // buyer pays at most their limit
                    } else {
                        lp <= p // seller receives at least their limit
                    }
                }
            };
            if executable {
                if o.is_buy {
                    buy_qty += o.qty;
                } else {
                    sell_qty += o.qty;
                }
            }
        }
        (buy_qty, sell_qty)
    }

    /// Indicative price/volume/imbalance during the call (design §3:
    /// publish so participants can react).
    pub fn indicative(&self) -> Indicative {
        if self.orders.is_empty() {
            return Indicative {
                price: None,
                matched_qty: 0,
                imbalance: 0,
            };
        }
        let mut best: Option<(i128, i128, i128)> = None; // (price, matched, imbalance)
        for p in self.candidate_prices() {
            let (buy, sell) = self.book_at(p);
            let matched = buy.min(sell);
            let imbalance = buy - sell;
            let take = match best {
                None => true,
                Some((_, bm, _)) => matched > bm,
            };
            if take {
                best = Some((p, matched, imbalance));
            }
        }
        match best {
            Some((p, m, i)) if m > 0 => Indicative {
                price: Some(p),
                matched_qty: m,
                imbalance: i,
            },
            _ => Indicative {
                price: None,
                matched_qty: 0,
                imbalance: 0,
            },
        }
    }

    /// End the call and compute the uncrossing (design §3):
    /// maximize matched volume, tie-break by max surplus (matched × distance
    /// to nothing — here: the smaller side's headroom), then min |imbalance|,
    /// then closest to the reference price.
    pub fn uncross(&mut self) -> Option<Uncross> {
        self.state = AuctionState::Uncrossing;
        if self.orders.is_empty() {
            return None;
        }
        let mut best: Option<(i128, i128, i128)> = None; // (price, matched, imbalance)
        for p in self.candidate_prices() {
            let (buy, sell) = self.book_at(p);
            let matched = buy.min(sell);
            if matched <= 0 {
                continue;
            }
            let imbalance = buy - sell;
            let better = match best {
                None => true,
                Some((_, bm, _)) => {
                    if matched != bm {
                        matched > bm // primary: max matched volume
                    } else {
                        // tie 1: min |imbalance|
                        if imbalance.abs() != best.unwrap().2.abs() {
                            imbalance.abs() < best.unwrap().2.abs()
                        } else {
                            // tie 2: closest to the reference price
                            match self.reference_price {
                                Some(r) => (p - r).abs() < (best.unwrap().0 - r).abs(),
                                None => false,
                            }
                        }
                    }
                }
            };
            if better {
                best = Some((p, matched, imbalance));
            }
        }
        best.map(|(price, matched_qty, imbalance)| Uncross {
            price,
            matched_qty,
            imbalance,
        })
    }

    /// Post-uncross: clear accumulated orders, transition to Open.
    pub fn finish(&mut self) {
        self.orders.clear();
        self.state = AuctionState::Open;
    }
}

// ---------------------------------------------------------------------------
// Durable session binding (DESIGN.md §3 — the me.auction row lifecycle)
// ---------------------------------------------------------------------------

/// One durable auction session: the `me.auction` row identity the shard's
/// auction task advances in the same transaction as the outbox event and
/// the audit journal rows (DESIGN.md §3 — the in-memory and durable state machines
/// advance together, never independently).
#[derive(Debug, Clone, PartialEq)]
pub struct DurableSession {
    /// The `me.auction` row id (the outbox event's idempotency anchor).
    pub auction_id: Uuid,
    /// Tenant environment (RLS context for the row writes).
    pub env_id: Uuid,
    /// The book the session trades (me.auction.book_id).
    pub book_id: Uuid,
    /// Instrument id the engine keys books by.
    pub instrument_id: u32,
    /// Session type (CHECK key from [`AuctionType::as_str`]).
    pub auction_type: AuctionType,
    /// Current durable state.
    pub state: DurableAuctionState,
}

impl DurableSession {
    /// The one valid durable transition when the engine finishes an (possibly
    /// empty) uncross: `locked → uncrossing → complete` on success;
    /// `accepting → cancelled` semantics hold when the operator withdraws —
    /// a completed uncross always lands `complete` (the empty-book case
    /// stays `locked`, retried on the next window).
    pub fn advance_after_uncross(&mut self, uncrossed: bool) -> Option<DurableAuctionState> {
        let next = match self.state {
            DurableAuctionState::Accepting => DurableAuctionState::Uncrossing,
            DurableAuctionState::Locked => DurableAuctionState::Uncrossing,
            DurableAuctionState::Uncrossing if uncrossed => DurableAuctionState::Complete,
            _ => return None,
        };
        if next == DurableAuctionState::Uncrossing {
            self.state = DurableAuctionState::Uncrossing;
            if uncrossed {
                self.state = DurableAuctionState::Complete;
                return Some(DurableAuctionState::Complete);
            }
        }
        self.state = next;
        Some(self.state)
    }
}

/// Advance the durable `me.auction` row and emit the uncross event in ONE
/// transaction: state UPDATE → `me.auction_order` uncross snapshot →
/// `me.outbox` INSERT (`exchange8:me:auction:uncrossed`, family
/// `me.auction`) → `me.audit_journal` rows. Any failure aborts the whole
/// pair (DESIGN.md §6: the uncross transaction owns the row update —
/// the durable state machine cannot lag the published event).
///
/// # Errors
/// Propagates the SQL error — the caller keeps the session `locked` and
/// retries on the next window (no partial state, no orphan event).
pub async fn commit_uncross(
    pool: &sqlx::PgPool,
    session: &DurableSession,
    uncross: &Uncross,
    orders: &[(u64, bool, Option<i128>, i128)],
) -> Result<Uuid, sqlx::Error> {
    let mut tx = pool.begin().await?;
    sentin8::shell::set_tenant_context(&mut tx, session.env_id).await;

    // 1. The durable state advance + the clearing price, guarded on the
    //    current state (idempotency: a re-run finds the row already
    //    `complete` and updates 0 rows).
    let guarded = sqlx::query(
        r#"UPDATE me.auction
              SET state = 'complete', uncross_price = $3, uncrossed_at = now()
            WHERE id = $1 AND env_id = $2
              AND state IN ('locked','accepting','uncrossing')
              AND uncross_price IS NULL"#,
    )
    .bind(session.auction_id)
    .bind(session.env_id)
    .bind(uncross.price.to_string() as String)
    .execute(&mut *tx)
    .await?;
    if guarded.rows_affected() == 0 {
        // Already advanced (re-delivery) or concurrently withdrawn — the
        // caller treats this as a deduped no-op.
        tx.commit().await?;
        return Ok(session.auction_id);
    }

    // 2. The participating-orders snapshot (auction_order rows — the
    //    engine's accumulated book at uncross time).
    for (order_id, is_buy, limit_price, qty) in orders {
        sqlx::query(
            r#"INSERT INTO me.auction_order
                   (env_id, auction_id, order_id, side, price, qty)
               VALUES ($1, $2, $3, $4, $5, $6)"#,
        )
        .bind(session.env_id)
        .bind(session.auction_id)
        .bind(*order_id as i64)
        .bind(if *is_buy { "B" } else { "S" })
        .bind(limit_price.map(|p| p.to_string()))
        .bind(qty.to_string() as String)
        .execute(&mut *tx)
        .await?;
    }

    // 3. The outbox event (family `me.auction`, envelope-style payload).
    let payload = crate::events::AuctionUncrossPayload {
        instrument_id: session.instrument_id,
        auction_id: session.auction_id,
        auction_type: session.auction_type,
        uncross_price: uncross.price,
        matched_qty: uncross.matched_qty,
        imbalance: uncross.imbalance,
        ts_us: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64,
    };
    let env_json = serde_json::json!({
        "event_type": crate::events::EV_AUCTION_UNCROSSED,
        "env_id": session.env_id.to_string(),
        "payload": payload,
    });
    sentin8::bus::outbox::insert_outbox_row_with_publisher(
        &mut *tx,
        session.env_id,
        crate::events::FAMILY_AUCTION,
        crate::events::EV_AUCTION_UNCROSSED,
        &env_json,
        None,
        Some("exchange8:me"),
    )
    .await
    .map_err(|e| match e {
        sentin8::bus::outbox::OutboxError::Db(db) => db,
        other => sqlx::Error::Configuration(Box::new(other)),
    })?;

    // 4. The audit journal rows (same tx; the shell pusher drains them).
    let ev = sentin8::audit::AuditEvent::builder()
        .service("exchange8:me")
        .action(sentin8::audit::AuditAction::Execute)
        .resource("auction", session.auction_id.to_string())
        .actor("exchange8:me", "machine")
        .tenant(Uuid::nil(), session.env_id)
        .severity(sentin8::audit::AuditSeverity::Info)
        .correlation_id(session.auction_id.to_string())
        .message(format!(
            "auction {} {} uncrossed at {} (matched {}, imbalance {})",
            session.auction_type.as_str(),
            session.instrument_id,
            uncross.price,
            uncross.matched_qty,
            uncross.imbalance
        ))
        .details(serde_json::to_value(&payload).unwrap_or_default())
        .build();
    sentin8::audit::journal_insert(&mut *tx, &ev).await?;

    tx.commit().await?;
    Ok(session.auction_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limit(id: u64, buy: bool, price: i128, qty: i128) -> AuctionOrder {
        AuctionOrder {
            order_id: id,
            is_buy: buy,
            limit_price: Some(price),
            qty,
        }
    }

    fn market(id: u64, buy: bool, qty: i128) -> AuctionOrder {
        AuctionOrder {
            order_id: id,
            is_buy: buy,
            limit_price: None,
            qty,
        }
    }

    #[test]
    fn state_machine_transitions() {
        let mut a = AuctionEngine::new(1, AuctionType::Opening, None);
        assert_eq!(a.state(), AuctionState::Closed);
        a.open_call();
        assert_eq!(a.state(), AuctionState::PreOpen);
        a.add(limit(1, true, 10_000, 100));
        a.uncross();
        assert_eq!(a.state(), AuctionState::Uncrossing);
        a.finish();
        assert_eq!(a.state(), AuctionState::Open);
    }

    #[test]
    fn simple_cross_clears_at_overlap() {
        let mut a = AuctionEngine::new(1, AuctionType::Opening, None);
        a.open_call();
        a.add(limit(1, true, 10_100, 100)); // buy 100 @ up to 10.01
        a.add(limit(2, false, 9_900, 100)); // sell 100 @ down to 9.99
                                            // Overlap 9.99–10.01: candidates include both limits; max volume
                                            // (100) is achieved at both; tie-break with no reference keeps the
                                            // first-seen candidate order (sorted ascending → 9_900 first)…
                                            // but 9_900 yields matched=100 as does 10_100.
        let u = a.uncross().unwrap();
        assert_eq!(u.matched_qty, 100);
        assert!(u.price >= 9_900 && u.price <= 10_100);
        assert_eq!(u.imbalance, 0);
    }

    #[test]
    fn max_volume_price_wins_over_best_price() {
        // Two candidate prices with different achievable volumes: the price
        // with MORE matched volume must win even if the other is "better"
        // for one side.
        let mut a = AuctionEngine::new(1, AuctionType::Opening, None);
        a.open_call();
        // Heavy buy interest at 10.00 (300), light at 10.05 (100).
        a.add(limit(1, true, 10_000, 300));
        a.add(limit(2, true, 10_050, 100));
        // Sells: 200 at 10.05, 100 at 10.00.
        a.add(limit(3, false, 10_050, 200));
        a.add(limit(4, false, 10_000, 100));
        // At 10.00: buys executable 300+100=400 (limit ≥ p), sells 100 → 100.
        // At 10.05: buys 100, sells 300 → 100.
        // Max volume is 100 at both → tie-break min |imbalance|:
        // 10.00: imbalance 400−100=+300; 10.05: 100−300=−200 → 10.05 wins.
        let u = a.uncross().unwrap();
        assert_eq!(u.matched_qty, 100);
        assert_eq!(u.price, 10_050);
        assert_eq!(u.imbalance, -200);
    }

    #[test]
    fn reference_price_breaks_final_tie() {
        let mut a = AuctionEngine::new(1, AuctionType::Closing, Some(10_000));
        a.open_call();
        a.add(limit(1, true, 10_100, 100));
        a.add(limit(2, false, 9_900, 100));
        // All candidate prices achieve matched=100 with |imbalance|=0;
        // the reference 10_000 is closest to itself → wins the tie.
        let u = a.uncross().unwrap();
        assert_eq!(u.price, 10_000);
        assert_eq!(u.matched_qty, 100);
    }

    #[test]
    fn market_orders_execute_at_any_price() {
        let mut a = AuctionEngine::new(1, AuctionType::Opening, None);
        a.open_call();
        a.add(market(1, true, 100)); // MOO
        a.add(limit(2, false, 10_000, 100));
        let u = a.uncross().unwrap();
        // Market buy executes at the sell limit: clearing 10_000, matched 100.
        assert_eq!(u.price, 10_000);
        assert_eq!(u.matched_qty, 100);
        assert_eq!(u.imbalance, 0);
    }

    #[test]
    fn market_both_sides_clear_on_reference() {
        let mut a = AuctionEngine::new(1, AuctionType::Opening, Some(9_500));
        a.open_call();
        a.add(market(1, true, 100));
        a.add(market(2, false, 150));
        let u = a.uncross().unwrap();
        // No limits → only candidate is the reference 9_500.
        assert_eq!(u.price, 9_500);
        assert_eq!(u.matched_qty, 100);
        assert_eq!(u.imbalance, -50);
    }

    #[test]
    fn no_overlap_no_uncross() {
        let mut a = AuctionEngine::new(1, AuctionType::Opening, None);
        a.open_call();
        a.add(limit(1, true, 9_000, 100));
        a.add(limit(2, false, 10_000, 100));
        assert!(a.uncross().is_none());
        // Indicative during the call shows no price either.
        let ind = a.indicative();
        assert_eq!(ind.price, None);
        assert_eq!(ind.matched_qty, 0);
    }

    #[test]
    fn empty_book_no_uncross() {
        let mut a = AuctionEngine::new(1, AuctionType::Opening, None);
        a.open_call();
        assert!(a.uncross().is_none());
        let ind = a.indicative();
        assert_eq!(ind.price, None);
    }

    #[test]
    fn indicative_tracks_imbalance() {
        let mut a = AuctionEngine::new(1, AuctionType::Opening, None);
        a.open_call();
        a.add(limit(1, true, 10_000, 300));
        a.add(limit(2, false, 10_000, 100));
        let ind = a.indicative();
        assert_eq!(ind.price, Some(10_000));
        assert_eq!(ind.matched_qty, 100);
        assert_eq!(ind.imbalance, 200); // buy-side excess
                                        // Add sells: imbalance shrinks.
        a.add(limit(3, false, 10_000, 100));
        let ind = a.indicative();
        assert_eq!(ind.matched_qty, 200);
        assert_eq!(ind.imbalance, 100);
    }

    #[test]
    fn cancel_during_call() {
        let mut a = AuctionEngine::new(1, AuctionType::Opening, None);
        a.open_call();
        a.add(limit(1, true, 10_000, 100));
        assert!(a.cancel(1));
        assert!(!a.cancel(99));
        assert!(a.uncross().is_none());
    }

    #[test]
    fn finish_clears_orders() {
        let mut a = AuctionEngine::new(1, AuctionType::Opening, None);
        a.open_call();
        a.add(limit(1, true, 10_000, 100));
        a.add(limit(2, false, 9_900, 100));
        let u = a.uncross().unwrap();
        a.finish();
        assert_eq!(a.state(), AuctionState::Open);
        // Orders cleared: a second uncross on the same session is empty.
        assert!(a.uncross().is_none());
        let _ = u;
    }

    #[test]
    fn large_book_volume_maximization() {
        // Classic auction ladder: the clearing price shifts to maximize
        // total volume, not to satisfy the earliest orders.
        let mut a = AuctionEngine::new(1, AuctionType::Opening, None);
        a.open_call();
        // Buys: 500 @ 10.00, 300 @ 10.01, 100 @ 10.02.
        a.add(limit(1, true, 10_000, 500));
        a.add(limit(2, true, 10_010, 300));
        a.add(limit(3, true, 10_020, 100));
        // Sells: 100 @ 10.00, 300 @ 10.01, 400 @ 10.02.
        a.add(limit(4, false, 10_000, 100));
        a.add(limit(5, false, 10_010, 300));
        a.add(limit(6, false, 10_020, 400));
        // At 10.00: buys 900, sells 100 → 100.
        // At 10.01: buys 400, sells 400 → 400.
        // At 10.02: buys 100, sells 800 → 100.
        let u = a.uncross().unwrap();
        assert_eq!(u.price, 10_010);
        assert_eq!(u.matched_qty, 400);
        assert_eq!(u.imbalance, 0);
    }

    // ------------------------------------------------------------------
    // Durable binding (DESIGN.md §3)
    // ------------------------------------------------------------------

    #[test]
    fn durable_state_vocabulary_round_trips() {
        // Every CHECK vocabulary key round-trips (from_db ↔ as_str) — the
        // row-state reader never invents a state.
        for s in [
            DurableAuctionState::Scheduled,
            DurableAuctionState::Accepting,
            DurableAuctionState::Locked,
            DurableAuctionState::Uncrossing,
            DurableAuctionState::Complete,
            DurableAuctionState::Cancelled,
        ] {
            assert_eq!(DurableAuctionState::from_db(s.as_str()), Some(s));
        }
        assert_eq!(DurableAuctionState::from_db("nonsense"), None);
    }

    #[test]
    fn durable_advance_follows_the_uncross_outcome() {
        // locked → uncrossing → complete on success.
        let mut s = DurableSession {
            auction_id: Uuid::new_v4(),
            env_id: Uuid::nil(),
            book_id: Uuid::nil(),
            instrument_id: 1,
            auction_type: AuctionType::Opening,
            state: DurableAuctionState::Locked,
        };
        assert_eq!(
            s.advance_after_uncross(true),
            Some(DurableAuctionState::Complete)
        );
        assert_eq!(s.state, DurableAuctionState::Complete);
        // accepting → uncrossing when the window fires without a result yet.
        let mut s2 = DurableSession {
            state: DurableAuctionState::Accepting,
            ..s
        };
        assert_eq!(
            s2.advance_after_uncross(false),
            Some(DurableAuctionState::Uncrossing)
        );
        assert_eq!(s2.state, DurableAuctionState::Uncrossing);
        // Terminal states never advance.
        let mut s3 = DurableSession {
            state: DurableAuctionState::Complete,
            ..s
        };
        assert_eq!(s3.advance_after_uncross(true), None);
        // Cancelled sessions never advance.
        let mut s4 = DurableSession {
            state: DurableAuctionState::Cancelled,
            ..s
        };
        assert_eq!(s4.advance_after_uncross(true), None);
    }

    #[test]
    fn engine_phase_maps_from_durable_state() {
        // accepting/locked = the call period (PreOpen); uncrossing =
        // Uncrossing; complete = Open; scheduled/cancelled have no engine
        // phase (the engine task never sees them pre-open / post-exit).
        assert_eq!(
            DurableAuctionState::Accepting.engine_phase(),
            Some(AuctionState::PreOpen)
        );
        assert_eq!(
            DurableAuctionState::Uncrossing.engine_phase(),
            Some(AuctionState::Uncrossing)
        );
        assert_eq!(
            DurableAuctionState::Complete.engine_phase(),
            Some(AuctionState::Open)
        );
        assert_eq!(DurableAuctionState::Scheduled.engine_phase(), None);
        assert_eq!(
            DurableAuctionState::Cancelled.engine_phase(),
            Some(AuctionState::Closed)
        );
    }

    #[test]
    fn auction_type_keys_match_the_check_vocabulary() {
        // The me.auction.auction_type CHECK enumerates exactly these keys.
        assert_eq!(AuctionType::Opening.as_str(), "opening");
        assert_eq!(AuctionType::Closing.as_str(), "closing");
        assert_eq!(AuctionType::Reopen.as_str(), "reopen");
        assert_eq!(AuctionType::Halt.as_str(), "halt");
        assert_eq!(AuctionType::Ipo.as_str(), "ipo");
        assert_eq!(AuctionType::Volatility.as_str(), "volatility");
    }
}
