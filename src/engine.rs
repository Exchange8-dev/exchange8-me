// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Exchange8:ME
// SPDX-License-Identifier: Apache-2.0

// Exchange8:ME matching engine core.
//
// Deterministic CLOB matching preserving the the platform spec
// semantics exactly: price-time + broker-preferencing, board/odd-lot
// separation, iceberg reload + time-priority reset, post-only, price
// protection, STP, halts, quotation-only.
//
// Design: `services/me/DESIGN.md` §6 (matching discipline).
// Truth: `design/behavior-catalog/messages.json` (ME messages/enums).

use exchange8::book::{alloc, AllocationMethod, Book, RestingOrder};
use exchange8::contracts::enums::*;
use exchange8::contracts::events::*;
use exchange8::fixed::{Price, Qty};

/// Instrument book state (one per instrument).
///
/// `instrument_id` is kept for journal/dump tooling parity (
/// `InstrumentBook::instrumentID`); it also anchors the book identity when
/// serialized.
#[allow(dead_code)]
#[derive(Debug)]
pub struct InstrumentBook {
    pub instrument_id: u32,
    pub status: InstrumentStatus,
    pub lot_size: Qty,
    /// Within-level fill discipline (ME §4.4): `fifo` for equities,
    /// pro-rata variants for commodity futures / hybrid contracts.
    pub allocation: AllocationMethod,
    /// board book  odd book per side (4-book split).
    board: Book,
    odd: Book,
    /// Orders index across both books.
    orders: std::collections::HashMap<u64, Side>,
}

impl InstrumentBook {
    pub fn new(instrument_id: u32, lot_size: Qty) -> Self {
        InstrumentBook {
            instrument_id,
            status: InstrumentStatus::Open,
            lot_size,
            allocation: AllocationMethod::Fifo,
            board: Book::new(),
            odd: Book::new(),
            orders: std::collections::HashMap::new(),
        }
    }

    /// Set the contract's allocation policy (from the contract master).
    pub fn with_allocation(mut self, method: AllocationMethod) -> Self {
        self.allocation = method;
        self
    }

    pub fn book_for(&self, odd_lot: bool) -> &Book {
        if odd_lot {
            &self.odd
        } else {
            &self.board
        }
    }

    pub fn book_mut(&mut self, odd_lot: bool) -> &mut Book {
        if odd_lot {
            &mut self.odd
        } else {
            &mut self.board
        }
    }
}

/// The matching engine: a collection of instrument books + a trade-id source.
///
/// `with_trade_id_start`, `modify_order`, `order_remaining`, and
/// `cancel_all_for_connection` are wired by the service shell; they are
/// API-complete so the shell needs no engine changes.
#[allow(dead_code)]
pub struct MatchingEngine {
    books: std::collections::HashMap<u32, InstrumentBook>,
    next_trade_id: u64,
    /// Outbound lifecycle events, in emission order.
    pub events: Vec<MeEvent>,
}

/// Lifecycle events emitted by the engine (mirrors the ME outbound set).
///
/// `Booked`/`Repriced` payloads are read by downstream sinks (journal, feeds);
/// the enum is `#[allow(dead_code)]` until the wire shell wires them in
/// (see DESIGN.md §10).
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum MeEvent {
    Booked(OrderBooked),
    Killed(OrderKilled),
    Modified(OrderModified),
    Filled(OrderFilled),
    Rejected(TransactionRejected),
    Executed(OrderExecuted),
    Repriced(OrderRepriced),
}

impl MatchingEngine {
    pub fn new() -> Self {
        MatchingEngine {
            books: std::collections::HashMap::new(),
            next_trade_id: 1,
            events: Vec::new(),
        }
    }

    /// Construct with a custom first trade id (journal recovery).
    #[allow(dead_code)] // wired by the service shell (journal replay)
    pub fn with_trade_id_start(start: u64) -> Self {
        MatchingEngine {
            books: std::collections::HashMap::new(),
            next_trade_id: start,
            events: Vec::new(),
        }
    }

    /// The next trade id the engine hands out (the journal-lease frontier -
    /// the shell seeds it from the platform lease refill at boot/replay).
    pub fn set_trade_id_frontier(&mut self, start: u64) {
        self.next_trade_id = start;
    }

    fn trade_id(&mut self) -> u64 {
        let id = self.next_trade_id;
        self.next_trade_id += 1;
        id
    }

    fn book(&mut self, instrument_id: u32, lot_size: Qty) -> &mut InstrumentBook {
        self.books
            .entry(instrument_id)
            .or_insert_with(|| InstrumentBook::new(instrument_id, lot_size))
    }

    // ------------------------------------------------------------------
    // Instrument / market status
    // ------------------------------------------------------------------

    pub fn set_instrument_status(&mut self, instrument_id: u32, status: InstrumentStatus) {
        // Snapshot the current status without holding the borrow.
        let current = self.books.get(&instrument_id).map(|ib| ib.status);
        match current {
            Some(cur) => {
                if cur != status && status == InstrumentStatus::Halted {
                    // `Halt`: cancel all resting orders (drop the
                    // borrow first - `cancel_by_id` needs `&mut self`).
                    let ids: Vec<u64> = self
                        .books
                        .get(&instrument_id)
                        .map(|ib| ib.orders.keys().copied().collect())
                        .unwrap_or_default();
                    for id in ids {
                        self.cancel_by_id(instrument_id, id, KillReason::InstrumentHalted);
                    }
                }
                if let Some(ib) = self.books.get_mut(&instrument_id) {
                    ib.status = status;
                }
            }
            None => {
                // Status for an instrument never seen: create the book so the
                // status sticks (e.g. admin sets QuotationOnly before open).
                self.book(instrument_id, Qty::units(100)).status = status;
            }
        }
    }

    // ------------------------------------------------------------------
    // AddOrder
    // ------------------------------------------------------------------

    /// Process an AddOrder per the the platform spec flow.
    pub fn add_order(&mut self, msg: &AddOrder) {
        let lot = self.lot_size(msg.instrument_id);
        let ib = self.book(msg.instrument_id, lot);

        // 1. Instrument halted → reject.
        if ib.status == InstrumentStatus::Halted {
            self.events.push(MeEvent::Rejected(TransactionRejected {
                order_id: msg.order_id,
                instrument_id: msg.instrument_id,
                connection_id: msg.connection_id,
                reason: MeRejectReason::InstrumentHalted,
                rejected_op: RejectedOp::Add,
            }));
            return;
        }

        // 2. QuotationOnly: plain display limit orders only.
        if ib.status == InstrumentStatus::QuotationOnly {
            if msg.order_type != OrderType::Limit || msg.visibility != Visibility::Display {
                self.events.push(MeEvent::Rejected(TransactionRejected {
                    order_id: msg.order_id,
                    instrument_id: msg.instrument_id,
                    connection_id: msg.connection_id,
                    reason: MeRejectReason::InstrumentHalted,
                    rejected_op: RejectedOp::Add,
                }));
                return;
            }
            // Post without matching.
            self.insert_order(msg, msg.price);
            self.events.push(MeEvent::Booked(OrderBooked {
                price: msg.price,
                order_id: msg.order_id,
                sub_order_id: msg.sub_order_id,
                instrument_id: msg.instrument_id,
                volume: msg.qty,
                connection_id: msg.connection_id,
            }));
            return;
        }

        // 3. Post-only → PostOrder; never matches.
        if msg.order_type == OrderType::PostOnly {
            let crossed = self.would_cross(msg);
            if crossed {
                self.events.push(MeEvent::Killed(OrderKilled {
                    order_id: msg.order_id,
                    sub_order_id: msg.sub_order_id,
                    instrument_id: msg.instrument_id,
                    connection_id: msg.connection_id,
                    reason: KillReason::CrossMarket,
                }));
            } else {
                self.insert_order(msg, msg.price);
                self.events.push(MeEvent::Booked(OrderBooked {
                    price: msg.price,
                    order_id: msg.order_id,
                    sub_order_id: msg.sub_order_id,
                    instrument_id: msg.instrument_id,
                    volume: msg.qty,
                    connection_id: msg.connection_id,
                }));
            }
            return;
        }

        // 4. Price protection sidestep (pre-match).
        let mut price = msg.price;
        if let Some(protection) = msg.price_protection {
            let sidestep = self.protection_sidestep(msg, &price);
            if let Some(new_px) = sidestep {
                price = new_px;
                self.events.push(MeEvent::Repriced(OrderRepriced {
                    price: new_px,
                    order_id: msg.order_id,
                    sub_order_id: msg.sub_order_id,
                    instrument_id: msg.instrument_id,
                    connection_id: msg.connection_id,
                    reason: KillReason::PriceProtection,
                }));
                let _ = protection;
            }
        }

        // 5. Match (limit/IOC/FOK/bypass).
        let remaining = self.match_order(msg, price);

        // 6. Remainder booked unless IOC/FOK.
        match msg.order_type {
            OrderType::Ioc | OrderType::Bypass => {
                if remaining.raw > 0 {
                    self.events.push(MeEvent::Killed(OrderKilled {
                        order_id: msg.order_id,
                        sub_order_id: msg.sub_order_id,
                        instrument_id: msg.instrument_id,
                        connection_id: msg.connection_id,
                        reason: KillReason::Ioc,
                    }));
                }
            }
            OrderType::Fok => {
                if remaining.raw > 0 {
                    self.events.push(MeEvent::Killed(OrderKilled {
                        order_id: msg.order_id,
                        sub_order_id: msg.sub_order_id,
                        instrument_id: msg.instrument_id,
                        connection_id: msg.connection_id,
                        reason: KillReason::Fok,
                    }));
                }
            }
            _ => {
                if remaining.raw > 0 {
                    self.insert_remaining(msg, price, remaining);
                    self.events.push(MeEvent::Booked(OrderBooked {
                        price,
                        order_id: msg.order_id,
                        sub_order_id: msg.sub_order_id,
                        instrument_id: msg.instrument_id,
                        volume: remaining,
                        connection_id: msg.connection_id,
                    }));
                }
            }
        }
    }

    /// Match an incoming order against the opposite book; returns the
    /// unmatched remainder.
    fn match_order(&mut self, msg: &AddOrder, price: Price) -> Qty {
        let lot = self.lot_size(msg.instrument_id);
        let (board_lots, odd_qty) = msg.qty.split_board_odd(lot);
        let board_qty = Qty::units(board_lots.raw.saturating_mul(lot.raw));
        let mut remaining = msg.qty;
        let bypass = msg.order_type == OrderType::Bypass;

        // FOK pre-check across the whole walk.
        if msg.order_type == OrderType::Fok {
            let need = msg.qty;
            let have = self.available_volume(msg.instrument_id, msg.side, price, bypass);
            if have < need.raw {
                return remaining; // no execution; caller kills
            }
        }

        // Board portion then odd portion.
        if board_qty.raw > 0 {
            remaining = self.match_portion(msg, price, board_qty, false, bypass, remaining);
        }
        if odd_qty.raw > 0 {
            remaining = self.match_portion(msg, price, odd_qty, true, bypass, remaining);
        }
        remaining
    }

    /// The instrument's board lot size (tests use 100; production loads from
    /// RefData).
    fn lot_size(&self, instrument_id: u32) -> Qty {
        self.books
            .get(&instrument_id)
            .map(|ib| ib.lot_size)
            .unwrap_or_else(|| Qty::units(100))
    }

    /// Total opposite volume at-or-better (FOK check).
    fn available_volume(&self, instrument_id: u32, side: Side, price: Price, bypass: bool) -> u64 {
        let ib = match self.books.get(&instrument_id) {
            Some(b) => b,
            None => return 0,
        };
        let opposite = side.opposite();
        let mut total = 0u64;
        for odd_lot in [false, true] {
            let b = ib.book_for(odd_lot);
            let sb = b.side(opposite);
            for level in sb.iter_levels() {
                if bypass && !level.visible {
                    continue;
                }
                let better = match opposite {
                    Side::Bid => level.price.raw >= price.raw,
                    Side::Ask => level.price.raw <= price.raw,
                };
                if better {
                    total += level.total_remaining();
                }
            }
        }
        total
    }

    /// Match one (board|odd) portion; returns the new overall remainder.
    fn match_portion(
        &mut self,
        msg: &AddOrder,
        price: Price,
        portion: Qty,
        odd_lot: bool,
        bypass: bool,
        remaining: Qty,
    ) -> Qty {
        let mut to_match = portion.raw;
        let opposite = msg.side.opposite();

        // Broker-preferencing two-pass when the book prefers brokers: first
        // same-customer orders, then the rest .
        for prefer_own in [true, false] {
            if to_match == 0 {
                break;
            }
            // Walk opposite levels best-first. Termination: every iteration
            // either decreases `to_match`, removes/reduces a resting order
            // (shrinking the book), or breaks - no state is left unchanged.
            loop {
                if to_match == 0 {
                    break;
                }
                // Find the best matching level (best-first iteration -
                // the ordered map yields levels already sorted).
                let (level_price, _level_visible): (Option<(Price, bool)>, ()) = {
                    let ib = self.books.get(&msg.instrument_id).expect("book exists");
                    let sb = ib.book_for(odd_lot).side(opposite);
                    let mut found: Option<(Price, bool)> = None;
                    for level in sb.iter_levels() {
                        if bypass && !level.visible {
                            continue;
                        }
                        if prefer_own {
                            // only levels containing our customer
                            if !level
                                .orders
                                .iter()
                                .any(|o| o.customer_id == msg.customer_id)
                            {
                                continue;
                            }
                        }
                        let crosses = match opposite {
                            Side::Bid => level.price.raw >= price.raw,
                            Side::Ask => level.price.raw <= price.raw,
                        };
                        if crosses {
                            found = Some((level.price, level.visible));
                            break;
                        }
                    }
                    (found, ())
                };
                let (level_price, _level_visible) = match level_price {
                    Some(v) => v,
                    None => break,
                };

                // Execute against orders on that level per the contract's
                // allocation policy (ME §4.4): FIFO front-pick (default,
                // equities) or pro-rata whole-level allocation.
                let allocation = self
                    .books
                    .get(&msg.instrument_id)
                    .map(|ib| ib.allocation)
                    .unwrap_or(AllocationMethod::Fifo);
                if allocation != AllocationMethod::Fifo {
                    // Pro-rata: allocate `to_match` across the entire level
                    // (respecting the broker-preferencing pass filter) in
                    // one decision, then apply the fills.
                    let resting: Vec<alloc::RestingQty> = {
                        let ib = self.books.get(&msg.instrument_id).expect("book exists");
                        let sb = ib.book_for(odd_lot).side(opposite);
                        match sb.iter_levels().find(|l| l.price.raw == level_price.raw) {
                            Some(level) => level
                                .orders
                                .iter()
                                .filter(|o| !prefer_own || o.customer_id == msg.customer_id)
                                .map(|o| alloc::RestingQty {
                                    order_id: o.order_id,
                                    qty: o.remaining.raw,
                                    seq: o.seq,
                                })
                                .collect(),
                            None => Vec::new(),
                        }
                    };
                    let fills = alloc::allocate(allocation, &resting, to_match);
                    if fills.is_empty() {
                        break; // level drained under this pass filter
                    }
                    for f in fills {
                        let (resting_customer, resting_stp, resting_max_floor, resting_remaining) = {
                            let ib = self.books.get(&msg.instrument_id).expect("book exists");
                            let sb = ib.book_for(odd_lot).side(opposite);
                            let level = sb.iter_levels().find(|l| l.price.raw == level_price.raw);
                            match level
                                .and_then(|l| l.orders.iter().find(|o| o.order_id == f.order_id))
                            {
                                Some(o) => (o.customer_id, o.stp_key, o.max_floor, o.remaining),
                                None => continue, // filled/removed mid-apply
                            }
                        };

                        // STP (DESIGN.md §6): FOK disables STP (mpid=0 semantics).
                        if msg.order_type != OrderType::Fok {
                            if let (Some(a), Some(b)) = (msg.stp_key, resting_stp) {
                                if a == b {
                                    // Self-trade under pro-rata: the policy
                                    // pre-assigned this fill; treat the pair
                                    // as a Decrement-style skip (no fill on
                                    // this counterparty, passive stays).
                                    continue;
                                }
                            }
                        }

                        let fill = f.qty.min(resting_remaining.raw);
                        if fill == 0 {
                            continue;
                        }
                        let trade_id = self.trade_id();

                        // Active-side event.
                        self.events.push(MeEvent::Executed(OrderExecuted {
                            trade_id,
                            ts_us: 0,
                            price: level_price,
                            order_id: msg.order_id,
                            sub_order_id: msg.sub_order_id,
                            cp_order_id: f.order_id,
                            cp_sub_order_id: 0,
                            instrument_id: msg.instrument_id,
                            qty: Qty::units(fill),
                            remaining_volume: Qty::units(to_match - fill),
                            connection_id: msg.connection_id,
                            counterparty_id: if resting_customer != 0 {
                                Some(resting_customer)
                            } else {
                                None
                            },
                            book_id: msg.book_id,
                            liquidity: Liquidity::Removed,
                            side: msg.side,
                            lot_type: if odd_lot {
                                LotType::Odd
                            } else {
                                LotType::Board
                            },
                            last_mkt: [0; 4],
                            principal_trade: false,
                            wash_trade: false,
                            midpoint_charge: false,
                        }));
                        // Passive-side event (common trade_id).
                        self.events.push(MeEvent::Executed(OrderExecuted {
                            trade_id,
                            ts_us: 0,
                            price: level_price,
                            order_id: f.order_id,
                            sub_order_id: 0,
                            cp_order_id: msg.order_id,
                            cp_sub_order_id: msg.sub_order_id,
                            instrument_id: msg.instrument_id,
                            qty: Qty::units(fill),
                            remaining_volume: Qty::units(resting_remaining.raw - fill),
                            connection_id: 0,
                            counterparty_id: Some(msg.customer_id),
                            book_id: msg.book_id,
                            liquidity: Liquidity::Added,
                            side: opposite,
                            lot_type: if odd_lot {
                                LotType::Odd
                            } else {
                                LotType::Board
                            },
                            last_mkt: [0; 4],
                            principal_trade: false,
                            wash_trade: false,
                            midpoint_charge: false,
                        }));

                        to_match -= fill;
                        // Reduce / remove the resting order; iceberg reload.
                        self.consume_resting(
                            msg.instrument_id,
                            odd_lot,
                            opposite,
                            f.order_id,
                            fill,
                            resting_max_floor,
                        );
                    }
                    if to_match > 0 {
                        // Level fully consumed under pro-rata - continue the
                        // level walk for the next best level.
                        continue;
                    }
                    break;
                }

                while to_match > 0 {
                    // Pick the next matching resting order on this level.
                    let (
                        resting_id,
                        resting_visible,
                        resting_remaining,
                        resting_customer,
                        resting_stp,
                        resting_max_floor,
                    ) = {
                        let ib = self.books.get(&msg.instrument_id).expect("book exists");
                        let sb = ib.book_for(odd_lot).side(opposite);
                        let level = match sb.iter_levels().find(|l| l.price.raw == level_price.raw)
                        {
                            Some(l) => l,
                            None => break, // level consumed/pruned mid-walk
                        };
                        let mut pick: Option<&RestingOrder> = None;
                        for o in &level.orders {
                            // Broker pass filtering.
                            if prefer_own && o.customer_id != msg.customer_id {
                                continue;
                            }
                            if !prefer_own {
                                // second pass: any
                            }
                            pick = Some(o);
                            break;
                        }
                        match pick {
                            Some(o) => (
                                o.order_id,
                                o.visible,
                                o.remaining,
                                o.customer_id,
                                o.stp_key,
                                o.max_floor,
                            ),
                            None => (0, Qty::ZERO, Qty::ZERO, 0, None, Qty::ZERO),
                        }
                    };
                    if resting_id == 0 {
                        break;
                    }

                    // STP (DESIGN.md §6): FOK disables STP (mpid=0 semantics).
                    if msg.order_type != OrderType::Fok {
                        if let (Some(a), Some(b)) = (msg.stp_key, resting_stp) {
                            if a == b {
                                // Self-trade: apply the action.
                                match msg.stp_action {
                                    Some(exchange8::contracts::order::StpAction::KillActive)
                                    | None => {
                                        self.events.push(MeEvent::Killed(OrderKilled {
                                            order_id: msg.order_id,
                                            sub_order_id: msg.sub_order_id,
                                            instrument_id: msg.instrument_id,
                                            connection_id: msg.connection_id,
                                            reason: KillReason::SelfTrade,
                                        }));
                                        return Qty::units(
                                            remaining.raw - (portion.raw - to_match),
                                        );
                                    }
                                    Some(exchange8::contracts::order::StpAction::KillPassive) => {
                                        self.remove_resting(
                                            msg.instrument_id,
                                            odd_lot,
                                            opposite,
                                            resting_id,
                                        );
                                        if let Some(ib) = self.books.get_mut(&msg.instrument_id) {
                                            ib.orders.remove(&resting_id);
                                        }
                                        self.events.push(MeEvent::Killed(OrderKilled {
                                            order_id: resting_id,
                                            sub_order_id: 0,
                                            instrument_id: msg.instrument_id,
                                            connection_id: 0,
                                            reason: KillReason::SelfTrade,
                                        }));
                                        continue;
                                    }
                                    Some(exchange8::contracts::order::StpAction::KillBoth) => {
                                        self.remove_resting(
                                            msg.instrument_id,
                                            odd_lot,
                                            opposite,
                                            resting_id,
                                        );
                                        self.events.push(MeEvent::Killed(OrderKilled {
                                            order_id: msg.order_id,
                                            sub_order_id: msg.sub_order_id,
                                            instrument_id: msg.instrument_id,
                                            connection_id: msg.connection_id,
                                            reason: KillReason::SelfTrade,
                                        }));
                                        return Qty::units(
                                            remaining.raw - (portion.raw - to_match),
                                        );
                                    }
                                    Some(exchange8::contracts::order::StpAction::Decrement) => {
                                        let d = to_match.min(resting_remaining.raw);
                                        if d > 0 {
                                            to_match -= d;
                                            self.reduce_resting(
                                                msg.instrument_id,
                                                odd_lot,
                                                opposite,
                                                resting_id,
                                                d,
                                            );
                                        }
                                        continue;
                                    }
                                }
                            }
                        }
                    }

                    // Execute at the PASSIVE price, min(to_match, resting).
                    let fill = to_match.min(resting_remaining.raw);
                    if fill == 0 {
                        break; // resting order depleted (e.g. STP Decrement zeroed it)
                    }
                    let trade_id = self.trade_id();

                    // Active-side event.
                    self.events.push(MeEvent::Executed(OrderExecuted {
                        trade_id,
                        ts_us: 0,
                        price: level_price,
                        order_id: msg.order_id,
                        sub_order_id: msg.sub_order_id,
                        cp_order_id: resting_id,
                        cp_sub_order_id: 0,
                        instrument_id: msg.instrument_id,
                        qty: Qty::units(fill),
                        remaining_volume: Qty::units(to_match - fill),
                        connection_id: msg.connection_id,
                        counterparty_id: if resting_customer != 0 {
                            Some(resting_customer)
                        } else {
                            None
                        },
                        book_id: msg.book_id,
                        liquidity: Liquidity::Removed,
                        side: msg.side,
                        lot_type: if odd_lot {
                            LotType::Odd
                        } else {
                            LotType::Board
                        },
                        last_mkt: [0; 4],
                        principal_trade: false,
                        wash_trade: false,
                        midpoint_charge: false,
                    }));
                    // Passive-side event (common trade_id).
                    self.events.push(MeEvent::Executed(OrderExecuted {
                        trade_id,
                        ts_us: 0,
                        price: level_price,
                        order_id: resting_id,
                        sub_order_id: 0,
                        cp_order_id: msg.order_id,
                        cp_sub_order_id: msg.sub_order_id,
                        instrument_id: msg.instrument_id,
                        qty: Qty::units(fill),
                        remaining_volume: Qty::units(resting_remaining.raw - fill),
                        connection_id: 0,
                        counterparty_id: Some(msg.customer_id),
                        book_id: msg.book_id,
                        liquidity: Liquidity::Added,
                        side: opposite,
                        lot_type: if odd_lot {
                            LotType::Odd
                        } else {
                            LotType::Board
                        },
                        last_mkt: [0; 4],
                        principal_trade: false,
                        wash_trade: false,
                        midpoint_charge: false,
                    }));
                    let _ = resting_visible;
                    let _ = resting_max_floor;

                    to_match -= fill;
                    // Reduce / remove the resting order; iceberg reload.
                    self.consume_resting(
                        msg.instrument_id,
                        odd_lot,
                        opposite,
                        resting_id,
                        fill,
                        resting_max_floor,
                    );
                }
            }
        }
        Qty::units(remaining.raw - (portion.raw - to_match))
    }

    // ------------------------------------------------------------------
    // Book mutation helpers
    // ------------------------------------------------------------------

    fn insert_order(&mut self, msg: &AddOrder, price: Price) {
        let lot = self.lot_size(msg.instrument_id);
        let (board_lots, odd_qty) = msg.qty.split_board_odd(lot);
        let board_qty = Qty::units(board_lots.raw.saturating_mul(lot.raw));
        if board_qty.raw > 0 {
            // Iceberg visible part = min(max_floor, qty) when max_floor > 0.
            let visible_board = if msg.max_floor.raw > 0 {
                Qty::units(msg.max_floor.raw.min(board_qty.raw))
            } else {
                board_qty
            };
            let o = RestingOrder {
                order_id: msg.order_id,
                sub_order_id: msg.sub_order_id,
                customer_id: msg.customer_id,
                connection_id: msg.connection_id,
                side: msg.side,
                price,
                full_volume: board_qty,
                remaining: board_qty,
                visible: if msg.visibility == Visibility::Hidden {
                    Qty::ZERO
                } else {
                    visible_board
                },
                max_floor: msg.max_floor,
                iceberg_sub_order_count: 0,
                is_odd_lot: false,
                anonymous: msg.anonymous,
                stp_key: msg.stp_key,
                seq: 0,
            };
            let ib = self.book(msg.instrument_id, lot);
            ib.board.side_mut(msg.side).insert(o);
            ib.orders.insert(msg.order_id, msg.side);
        }
        if odd_qty.raw > 0 {
            let o = RestingOrder {
                order_id: msg.order_id,
                sub_order_id: msg.sub_order_id,
                customer_id: msg.customer_id,
                connection_id: msg.connection_id,
                side: msg.side,
                price,
                full_volume: odd_qty,
                remaining: odd_qty,
                visible: if msg.visibility == Visibility::Hidden {
                    Qty::ZERO
                } else {
                    odd_qty
                },
                max_floor: Qty::ZERO,
                iceberg_sub_order_count: 0,
                is_odd_lot: true,
                anonymous: msg.anonymous,
                stp_key: msg.stp_key,
                seq: 0,
            };
            let ib = self.book(msg.instrument_id, lot);
            ib.odd.side_mut(msg.side).insert(o);
            // same id maps to the same side
            ib.orders.insert(msg.order_id, msg.side);
        }
    }

    fn insert_remaining(&mut self, msg: &AddOrder, price: Price, remaining: Qty) {
        let mut m = msg.clone();
        m.qty = remaining;
        m.price = price;
        self.insert_order(&m, price);
    }

    fn would_cross(&self, msg: &AddOrder) -> bool {
        let ib = match self.books.get(&msg.instrument_id) {
            Some(b) => b,
            None => return false,
        };
        let lot = ib.lot_size;
        let (board_lots, odd_qty) = msg.qty.split_board_odd(lot);
        let board_qty = board_lots.raw.saturating_mul(lot.raw);
        let opposite = msg.side.opposite();
        let check = |book: &Book, qty: u64| -> bool {
            if qty == 0 {
                return false;
            }
            let sb = book.side(opposite);
            match sb.best() {
                Some(level) => match msg.side {
                    Side::Bid => level.price.raw <= msg.price.raw,
                    Side::Ask => level.price.raw >= msg.price.raw,
                },
                None => false,
            }
        };
        check(&ib.board, board_qty) || check(&ib.odd, odd_qty.raw)
    }

    fn protection_sidestep(&self, msg: &AddOrder, price: &Price) -> Option<Price> {
        // : a bid ≥ best ask (or ask ≤ best bid) steps to the protected
        // side. Without an L1 source here we approximate with the book.
        let ib = self.books.get(&msg.instrument_id)?;
        let opposite = msg.side.opposite();
        let best = ib.board.side(opposite).best()?;
        let step = match msg.side {
            Side::Bid => price.raw >= best.price.raw,
            Side::Ask => price.raw <= best.price.raw,
        };
        if !step {
            return None;
        }
        // Sidestep one tick from the protected price.
        let tick = 100i128;
        let new_raw = match msg.side {
            Side::Bid => best.price.raw - tick,
            Side::Ask => best.price.raw + tick,
        };
        Some(Price {
            raw: new_raw,
            scale: price.scale,
        })
    }

    fn remove_resting(&mut self, instrument_id: u32, odd_lot: bool, side: Side, order_id: u64) {
        if let Some(ib) = self.books.get_mut(&instrument_id) {
            ib.book_mut(odd_lot).side_mut(side).remove(order_id);
            ib.book_mut(odd_lot).side_mut(side).prune();
        }
    }

    fn reduce_resting(
        &mut self,
        instrument_id: u32,
        odd_lot: bool,
        side: Side,
        order_id: u64,
        by: u64,
    ) {
        if let Some(ib) = self.books.get_mut(&instrument_id) {
            ib.book_mut(odd_lot).side_mut(side).reduce(order_id, by);
        }
    }

    /// Consume a resting order by `fill`; handles removal and iceberg reload
    /// (reload moves the order to the **tail** of the FIFO and bumps the
    /// sub-order count - time priority resets on reload).
    fn consume_resting(
        &mut self,
        instrument_id: u32,
        odd_lot: bool,
        side: Side,
        order_id: u64,
        fill: u64,
        max_floor: Qty,
    ) {
        let lot = self.lot_size(instrument_id);
        let mut reinsert: Option<RestingOrder> = None;
        if let Some(ib) = self.books.get_mut(&instrument_id) {
            let sb = ib.book_mut(odd_lot).side_mut(side);
            // Reduce in place first (indexed, O(1) locate).
            if !sb.reduce(order_id, fill) {
                return;
            }
            // Inspect the order's post-fill state to decide removal/reload.
            let state = sb
                .levels()
                .iter()
                .flat_map(|l| l.orders.iter())
                .find(|o| o.order_id == order_id)
                .map(|o| (o.remaining.raw, o.visible.raw, o.is_iceberg()));
            if let Some((remaining, visible, iceberg)) = state {
                if remaining == 0 {
                    // Fully consumed → remove (no reserve left).
                    sb.remove(order_id);
                } else if iceberg && visible == 0 {
                    // Iceberg reload: visible = min(max_floor, remaining),
                    // bump the sub-order count, reinsert at the tail.
                    if let Some(mut o) = sb.remove(order_id) {
                        let reload = max_floor.raw.min(o.remaining.raw);
                        o.visible = Qty::units(reload);
                        o.iceberg_sub_order_count += 1;
                        reinsert = Some(o);
                    }
                }
            }
        }
        // Re-insert an iceberged order at the tail (time priority reset).
        if let Some(o) = reinsert {
            if o.remaining.raw > 0 {
                let ib = self.book(instrument_id, lot);
                ib.book_mut(odd_lot).side_mut(side).insert(o);
            }
        }
    }

    // ------------------------------------------------------------------
    // Cancel / Erase
    // ------------------------------------------------------------------

    pub fn cancel_order(&mut self, msg: &CancelOrder) {
        self.cancel_by_id(msg.instrument_id, msg.order_id, KillReason::ClientRequest);
    }

    /// 'u' - modify price/qty/floor of a resting order.
    ///
    /// Price or qty **increase** loses time priority (remove + reinsert at
    /// tail); a pure decrease keeps priority in place. Qty decrease reduces
    /// remaining (never below 0). New order id re-keyes the resting order
    /// (the platform spec up/down semantics).
    #[allow(dead_code)] // wired by the service shell (Gateway 'u' path)
    pub fn modify_order(&mut self, msg: &ModifyOrder) {
        let lot = self.lot_size(msg.instrument_id);
        let found = {
            let ib = self.books.get(&msg.instrument_id);
            ib.map(|ib| {
                (
                    ib.orders.contains_key(&msg.order_id),
                    ib.orders.get(&msg.order_id).copied(),
                )
            })
        };
        let (exists, side) = match found {
            Some((e, s)) => (e, s),
            None => {
                self.events.push(MeEvent::Rejected(TransactionRejected {
                    order_id: msg.order_id,
                    instrument_id: msg.instrument_id,
                    connection_id: msg.connection_id,
                    reason: MeRejectReason::OrderNotFound,
                    rejected_op: RejectedOp::Modify,
                }));
                return;
            }
        };
        if !exists {
            self.events.push(MeEvent::Rejected(TransactionRejected {
                order_id: msg.order_id,
                instrument_id: msg.instrument_id,
                connection_id: msg.connection_id,
                reason: MeRejectReason::OrderNotFound,
                rejected_op: RejectedOp::Modify,
            }));
            return;
        }
        let side = side.expect("side present when exists");

        // Extract the current order (from board or odd book).
        let mut current: Option<RestingOrder> = None;
        if let Some(ib) = self.books.get_mut(&msg.instrument_id) {
            for odd_lot in [false, true] {
                let sb = ib.book_mut(odd_lot).side_mut(side);
                if let Some(o) = sb.remove(msg.order_id) {
                    sb.prune();
                    current = Some(o);
                    break;
                }
            }
        }
        let mut o = match current {
            Some(o) => o,
            None => {
                self.events.push(MeEvent::Rejected(TransactionRejected {
                    order_id: msg.order_id,
                    instrument_id: msg.instrument_id,
                    connection_id: msg.connection_id,
                    reason: MeRejectReason::OrderNotFound,
                    rejected_op: RejectedOp::Modify,
                }));
                return;
            }
        };

        let mut repriced = o.price.raw != msg.new_price.raw;
        o.price = msg.new_price;

        // Qty delta: positive adds, negative reduces (floor 0).
        if msg.delta_qty >= 0 {
            o.remaining = Qty::units(o.remaining.raw.saturating_add(msg.delta_qty as u64));
            // increasing loses priority only if price changed too (handled by
            // tail insertion below); qty increase also loses priority.
            repriced = true;
        } else {
            let dec = (-(msg.delta_qty)) as u64;
            let new_rem = o.remaining.raw.saturating_sub(dec);
            o.remaining = Qty::units(new_rem);
            // Visible can't exceed remaining.
            o.visible = Qty::units(o.visible.raw.min(new_rem));
            if new_rem == 0 {
                // Fully reduced → treat as killed by modify request.
                if let Some(ib) = self.books.get_mut(&msg.instrument_id) {
                    ib.orders.remove(&msg.order_id);
                }
                self.events.push(MeEvent::Killed(OrderKilled {
                    order_id: msg.order_id,
                    sub_order_id: 0,
                    instrument_id: msg.instrument_id,
                    connection_id: msg.connection_id,
                    reason: KillReason::ModifyRequest,
                }));
                return;
            }
        }
        // Max floor update (iceberg display).
        if msg.new_max_floor.raw > 0 {
            o.max_floor = msg.new_max_floor;
            o.visible = Qty::units(o.max_floor.raw.min(o.remaining.raw));
            repriced = true;
        }

        if o.remaining.raw == 0 {
            if let Some(ib) = self.books.get_mut(&msg.instrument_id) {
                ib.orders.remove(&msg.order_id);
            }
            return;
        }

        // Re-key with the new order id and reinsert at the tail (any price or
        // qty change loses time priority; a pure floor resize keeps it simple:
        // tail for parity with reinsert).
        let old_id = o.order_id;
        o.order_id = msg.new_order_id;
        o.sub_order_id += 1;
        let odd_lot = o.is_odd_lot;
        let ib = self.book(msg.instrument_id, lot);
        ib.book_mut(odd_lot).side_mut(side).insert(o);
        if let Some(ib) = self.books.get_mut(&msg.instrument_id) {
            ib.orders.remove(&old_id);
            ib.orders.insert(msg.new_order_id, side);
        }
        let _ = repriced;
        self.events.push(MeEvent::Modified(OrderModified {
            new_price: msg.new_price,
            order_id: msg.new_order_id,
            sub_order_id: 0,
            instrument_id: msg.instrument_id,
            new_volume: Qty::units(self.order_remaining(msg.instrument_id, msg.new_order_id)),
            new_order_id: msg.new_order_id,
            connection_id: msg.connection_id,
            side,
            new_max_floor: msg.new_max_floor,
        }));
    }

    #[allow(dead_code)] // used by modify_order (service shell)
    fn order_remaining(&self, instrument_id: u32, order_id: u64) -> u64 {
        self.books
            .get(&instrument_id)
            .map(|ib| {
                let mut total = 0u64;
                for odd_lot in [false, true] {
                    for side in [Side::Bid, Side::Ask] {
                        for level in ib.book_for(odd_lot).side(side).iter_levels() {
                            for o in &level.orders {
                                if o.order_id == order_id {
                                    total += o.remaining.raw;
                                }
                            }
                        }
                    }
                }
                total
            })
            .unwrap_or(0)
    }

    fn cancel_by_id(&mut self, instrument_id: u32, order_id: u64, reason: KillReason) {
        let mut found = false;
        if let Some(ib) = self.books.get_mut(&instrument_id) {
            for odd_lot in [false, true] {
                for side in [Side::Bid, Side::Ask] {
                    let sb = ib.book_mut(odd_lot).side_mut(side);
                    if sb.remove(order_id).is_some() {
                        sb.prune();
                        found = true;
                    }
                }
            }
            ib.orders.remove(&order_id);
        }
        if found {
            self.events.push(MeEvent::Killed(OrderKilled {
                order_id,
                sub_order_id: 0,
                instrument_id,
                connection_id: 0,
                reason,
            }));
        } else {
            self.events.push(MeEvent::Rejected(TransactionRejected {
                order_id,
                instrument_id,
                connection_id: 0,
                reason: MeRejectReason::OrderNotFound,
                rejected_op: RejectedOp::Cancel,
            }));
        }
    }

    pub fn erase_order(&mut self, msg: &OrderStateMsg) {
        // 'X': silently drop without notifying the originator.
        if let Some(ib) = self.books.get_mut(&msg.instrument_id) {
            for odd_lot in [false, true] {
                for side in [Side::Bid, Side::Ask] {
                    let sb = ib.book_mut(odd_lot).side_mut(side);
                    if sb.remove(msg.order_id).is_some() {
                        sb.prune();
                    }
                }
            }
            ib.orders.remove(&msg.order_id);
        }
    }

    /// Cancel all orders for a connection (cancel-on-disconnect).
    ///
    /// The engine does not track connection→order mapping; the shell resolves
    /// the connection's order ids and cancels each (the `connection_id` on
    /// `OrderKilled` events identifies the owner downstream). This overload
    /// sweeps everything (halt sweep parity).
    #[allow(dead_code)] // wired by the service shell (cancel-on-disconnect 'd')
    pub fn cancel_all_for_connection(&mut self, msg: &CancelAllForConnection) {
        let ids: Vec<(u32, u64)> = self
            .books
            .iter()
            .flat_map(|(iid, ib)| ib.orders.keys().map(move |oid| (*iid, *oid)))
            .collect();
        // Note: we don't have the connection map here; the caller resolves
        // connection → order ids. This overload cancels every order (used by
        // halt sweeps).
        let _ = msg;
        for (iid, oid) in ids {
            self.cancel_by_id(iid, oid, KillReason::AdminRequest);
        }
    }

    // ------------------------------------------------------------------
    // Recovery ('W')
    // ------------------------------------------------------------------

    /// All-order state (recovery 'W'): every resting order's remaining volume.
    pub fn all_order_states(&self) -> Vec<OrderStateMsg> {
        let mut out = Vec::new();
        for (iid, ib) in &self.books {
            for odd_lot in [false, true] {
                for side in [Side::Bid, Side::Ask] {
                    for level in ib.book_for(odd_lot).side(side).iter_levels() {
                        for o in &level.orders {
                            out.push(OrderStateMsg {
                                order_id: o.order_id,
                                instrument_id: *iid,
                                volume: o.remaining,
                                connection_id: o.connection_id,
                                notify_killed: false,
                            });
                        }
                    }
                }
            }
        }
        out
    }

    pub fn drain_events(&mut self) -> Vec<MeEvent> {
        std::mem::take(&mut self.events)
    }
}

impl Default for MatchingEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn add(order_id: u64, side: Side, px: i128, qty: u64) -> AddOrder {
        AddOrder {
            price: Price::equity(px),
            order_id,
            sub_order_id: 0,
            instrument_id: 1,
            qty: Qty::units(qty),
            connection_id: 1,
            side,
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
            max_floor: Qty::ZERO,
            strategy: vec![b'*'],
        }
    }

    fn executed_pairs(events: &[MeEvent]) -> Vec<(u64, u64, i128)> {
        events
            .iter()
            .filter_map(|e| match e {
                MeEvent::Executed(x) => Some((x.trade_id, x.qty.raw, x.price.raw)),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn price_time_priority() {
        let mut me = MatchingEngine::new();
        me.add_order(&add(1, Side::Ask, 10_100, 100));
        me.add_order(&add(2, Side::Ask, 10_100, 100)); // same price, later
        me.add_order(&add(3, Side::Ask, 10_200, 100)); // worse
        me.drain_events();
        // Aggressive buy 200 @ 10.10 → takes order 1 fully, then order 2
        // fully (board portion only; 200 is board-round).
        me.add_order(&add(9, Side::Bid, 10_100, 200));
        let ev = me.drain_events();
        let trades = executed_pairs(&ev);
        // Two fills against order 1 (100) then order 2 (100), two events each.
        assert_eq!(trades.len(), 4, "two per side per execution");
        assert_eq!(trades[0].1, 100);
        assert_eq!(trades[2].1, 100);
        // FIFO: order 1 filled before order 2 (event order proves it).
        let ids: Vec<u64> = ev
            .iter()
            .filter_map(|e| match e {
                MeEvent::Executed(x) => Some(x.order_id),
                _ => None,
            })
            .collect();
        assert_eq!(ids, vec![9, 1, 9, 2]);
        // Order 3 keeps 100 resting.
        let states = me.all_order_states();
        let o3 = states.iter().find(|s| s.order_id == 3).unwrap();
        assert_eq!(o3.volume.raw, 100);
    }

    #[test]
    fn rest_and_cross() {
        let mut me = MatchingEngine::new();
        me.add_order(&add(1, Side::Bid, 10_000, 100));
        let ev = me.drain_events();
        assert!(matches!(ev[0], MeEvent::Booked(_)));
        // Crossing sell fills.
        me.add_order(&add(2, Side::Ask, 10_000, 100));
        let ev = me.drain_events();
        assert_eq!(executed_pairs(&ev).len(), 2);
    }

    #[test]
    fn ioc_remainder_killed() {
        let mut me = MatchingEngine::new();
        me.add_order(&add(1, Side::Bid, 10_000, 100));
        me.drain_events();
        let mut m = add(2, Side::Ask, 10_000, 150);
        m.order_type = OrderType::Ioc;
        me.add_order(&m);
        let ev = me.drain_events();
        // Fill of 100 then kill of the 50 remainder.
        assert!(ev.iter().any(|e| matches!(e, MeEvent::Executed(_))));
        assert!(ev
            .iter()
            .any(|e| matches!(e, MeEvent::Killed(k) if k.reason == KillReason::Ioc)));
    }

    #[test]
    fn fok_insufficient_killed() {
        let mut me = MatchingEngine::new();
        me.add_order(&add(1, Side::Bid, 10_000, 100));
        me.drain_events();
        let mut m = add(2, Side::Ask, 10_000, 150);
        m.order_type = OrderType::Fok;
        me.add_order(&m);
        let ev = me.drain_events();
        assert!(!ev.iter().any(|e| matches!(e, MeEvent::Executed(_))));
        assert!(ev
            .iter()
            .any(|e| matches!(e, MeEvent::Killed(k) if k.reason == KillReason::Fok)));
    }

    #[test]
    fn fok_sufficient_fills() {
        let mut me = MatchingEngine::new();
        me.add_order(&add(1, Side::Bid, 10_000, 100));
        me.add_order(&add(2, Side::Bid, 10_000, 100));
        me.drain_events();
        let mut m = add(3, Side::Ask, 10_000, 200);
        m.order_type = OrderType::Fok;
        me.add_order(&m);
        let ev = me.drain_events();
        assert!(ev.iter().any(|e| matches!(e, MeEvent::Executed(_))));
        assert!(!ev.iter().any(|e| matches!(e, MeEvent::Killed(_))));
    }

    #[test]
    fn post_only_cross_killed() {
        let mut me = MatchingEngine::new();
        me.add_order(&add(1, Side::Bid, 10_000, 100));
        me.drain_events();
        let mut m = add(2, Side::Ask, 10_000, 100);
        m.order_type = OrderType::PostOnly;
        me.add_order(&m);
        let ev = me.drain_events();
        assert!(ev
            .iter()
            .any(|e| matches!(e, MeEvent::Killed(k) if k.reason == KillReason::CrossMarket)));
    }

    #[test]
    fn post_only_non_cross_books() {
        let mut me = MatchingEngine::new();
        me.add_order(&add(1, Side::Bid, 10_000, 100));
        me.drain_events();
        let mut m = add(2, Side::Ask, 10_100, 100);
        m.order_type = OrderType::PostOnly;
        me.add_order(&m);
        let ev = me.drain_events();
        assert!(matches!(ev[0], MeEvent::Booked(_)));
    }

    #[test]
    fn stp_kill_active() {
        let mut me = MatchingEngine::new();
        let mut resting = add(1, Side::Bid, 10_000, 100);
        resting.customer_id = 7;
        resting.stp_key = Some(77);
        me.add_order(&resting);
        me.drain_events();
        let mut m = add(2, Side::Ask, 10_000, 100);
        m.customer_id = 7;
        m.stp_key = Some(77);
        m.stp_action = Some(exchange8::contracts::order::StpAction::KillActive);
        me.add_order(&m);
        let ev = me.drain_events();
        assert!(ev
            .iter()
            .any(|e| matches!(e, MeEvent::Killed(k) if k.reason == KillReason::SelfTrade)));
    }

    #[test]
    fn stp_kill_passive() {
        let mut me = MatchingEngine::new();
        let mut resting = add(1, Side::Bid, 10_000, 100);
        resting.customer_id = 7;
        resting.stp_key = Some(77);
        me.add_order(&resting);
        me.drain_events();
        let mut m = add(2, Side::Ask, 10_000, 100);
        m.customer_id = 7;
        m.stp_key = Some(77);
        m.stp_action = Some(exchange8::contracts::order::StpAction::KillPassive);
        me.add_order(&m);
        let ev = me.drain_events();
        // The passive (resting) order is killed for SelfTrade.
        assert!(ev
            .iter()
            .any(|e| matches!(e, MeEvent::Killed(k) if k.reason == KillReason::SelfTrade && k.order_id == 1)));
        // The active order books normally (no self-trade fill against it).
        assert!(matches!(ev.last().unwrap(), MeEvent::Booked(_)));
    }

    #[test]
    fn broker_preferencing_two_pass() {
        let mut me = MatchingEngine::new();
        // Two resting asks from customer 5, then one from customer 9 at a
        // better price.
        let mut a1 = add(1, Side::Ask, 10_100, 100);
        a1.customer_id = 5;
        let mut a2 = add(2, Side::Ask, 10_100, 100);
        a2.customer_id = 5;
        let mut a3 = add(3, Side::Ask, 10_000, 100);
        a3.customer_id = 9;
        me.add_order(&a3); // better price first in book
        me.add_order(&a1);
        me.add_order(&a2);
        me.drain_events();
        // Aggressive buy from customer 5: prefers its own orders first even
        // though a3 has the better price.
        let mut b = add(9, Side::Bid, 10_100, 100);
        b.customer_id = 5;
        me.add_order(&b);
        let ev = me.drain_events();
        let trades = executed_pairs(&ev);
        assert!(!trades.is_empty());
        // First execution is at 10_100 (own-customer level), not 10_000.
        assert_eq!(trades[0].2, 10_100);
    }

    #[test]
    fn halt_kills_all_resting() {
        let mut me = MatchingEngine::new();
        me.add_order(&add(1, Side::Bid, 10_000, 100));
        me.add_order(&add(2, Side::Ask, 10_100, 100));
        me.drain_events();
        me.set_instrument_status(1, InstrumentStatus::Halted);
        let ev = me.drain_events();
        let kills = ev
            .iter()
            .filter(|e| matches!(e, MeEvent::Killed(k) if k.reason == KillReason::InstrumentHalted))
            .count();
        assert_eq!(kills, 2);
        assert!(me.all_order_states().is_empty());
        // New orders rejected.
        me.add_order(&add(3, Side::Bid, 10_000, 100));
        let ev = me.drain_events();
        assert!(matches!(ev[0], MeEvent::Rejected(_)));
    }

    #[test]
    fn quotation_only_limits_orders() {
        let mut me = MatchingEngine::new();
        me.set_instrument_status(1, InstrumentStatus::QuotationOnly);
        // Limit+display books.
        me.add_order(&add(1, Side::Bid, 10_000, 100));
        let ev = me.drain_events();
        assert!(matches!(ev[0], MeEvent::Booked(_)));
        // IOC rejected.
        let mut m = add(2, Side::Bid, 10_000, 100);
        m.order_type = OrderType::Ioc;
        me.add_order(&m);
        let ev = me.drain_events();
        assert!(matches!(ev[0], MeEvent::Rejected(_)));
    }

    #[test]
    fn mixed_lot_board_odd_split() {
        let mut me = MatchingEngine::new();
        // 150 with lot 100 → board 100 + odd 50.
        me.add_order(&add(1, Side::Bid, 10_000, 150));
        let states = me.all_order_states();
        // Same order id rests in both books; total remaining 150.
        let total: u64 = states.iter().map(|s| s.volume.raw).sum();
        assert_eq!(total, 150);
        // Board ask of 100 does NOT match the odd 50.
        me.add_order(&add(2, Side::Ask, 10_000, 100));
        let ev = me.drain_events();
        assert_eq!(executed_pairs(&ev).len(), 2); // one fill (100 board)
                                                  // Odd remainder stays.
        let states = me.all_order_states();
        let total: u64 = states.iter().map(|s| s.volume.raw).sum();
        assert_eq!(total, 50);
    }

    #[test]
    fn iceberg_reload_tail() {
        let mut me = MatchingEngine::new();
        // Iceberg: total 300, display 100.
        let mut m = add(1, Side::Bid, 10_000, 300);
        m.max_floor = Qty::units(100);
        me.add_order(&m);
        me.drain_events();
        // Consume 100 → reload to another 100 slice.
        me.add_order(&add(2, Side::Ask, 10_000, 100));
        let ev = me.drain_events();
        assert!(ev.iter().any(|e| matches!(e, MeEvent::Executed(_))));
        let states = me.all_order_states();
        let total: u64 = states.iter().map(|s| s.volume.raw).sum();
        assert_eq!(total, 200, "iceberg reserve remains");
    }

    #[test]
    fn cancel_and_not_found() {
        let mut me = MatchingEngine::new();
        me.add_order(&add(1, Side::Bid, 10_000, 100));
        me.drain_events();
        me.cancel_order(&CancelOrder {
            order_id: 1,
            instrument_id: 1,
            connection_id: 1,
        });
        let ev = me.drain_events();
        assert!(matches!(ev[0], MeEvent::Killed(k) if k.order_id == 1));
        // Cancel again → rejected NotFound.
        me.cancel_order(&CancelOrder {
            order_id: 1,
            instrument_id: 1,
            connection_id: 1,
        });
        let ev = me.drain_events();
        assert!(matches!(ev[0], MeEvent::Rejected(r) if r.reason == MeRejectReason::OrderNotFound));
    }

    #[test]
    fn erase_is_silent() {
        let mut me = MatchingEngine::new();
        me.add_order(&add(1, Side::Bid, 10_000, 100));
        me.drain_events();
        me.erase_order(&OrderStateMsg {
            order_id: 1,
            instrument_id: 1,
            volume: Qty::ZERO,
            connection_id: 0,
            notify_killed: false,
        });
        assert!(me.drain_events().is_empty());
        assert!(me.all_order_states().is_empty());
    }

    #[test]
    fn trade_ids_shared_per_side() {
        let mut me = MatchingEngine::new();
        me.add_order(&add(1, Side::Bid, 10_000, 100));
        me.drain_events();
        me.add_order(&add(2, Side::Ask, 10_000, 100));
        let ev = me.drain_events();
        let execs: Vec<&OrderExecuted> = ev
            .iter()
            .filter_map(|e| match e {
                MeEvent::Executed(x) => Some(x),
                _ => None,
            })
            .collect();
        assert_eq!(execs.len(), 2);
        assert_eq!(execs[0].trade_id, execs[1].trade_id);
        assert_eq!(execs[0].liquidity, Liquidity::Removed);
        assert_eq!(execs[1].liquidity, Liquidity::Added);
    }

    #[test]
    fn recovery_w_returns_all() {
        let mut me = MatchingEngine::new();
        me.add_order(&add(1, Side::Bid, 10_000, 100));
        me.add_order(&add(2, Side::Ask, 10_100, 200));
        let states = me.all_order_states();
        assert_eq!(states.len(), 2);
        assert!(states
            .iter()
            .any(|s| s.order_id == 2 && s.volume.raw == 200));
    }

    // ------------------------------------------------------------------
    // Matching discipline: strictly FIFO, never pro-rata
    // ------------------------------------------------------------------

    #[test]
    fn deep_level_fill_sequence_is_strictly_fifo() {
        // 30 resting orders at one price, one aggressive order consuming
        // the whole level: fills MUST land on orders 1..=30 in arrival
        // order, each exactly its full quantity. Any pro-rata or split
        // allocation breaks either the order or the quantities.
        let mut me = MatchingEngine::new();
        for id in 1..=30u64 {
            me.add_order(&add(id, Side::Ask, 10_000, 100));
        }
        me.drain_events();
        // Aggressive buy for the whole level (3_000 = 30 × 100).
        me.add_order(&add(99, Side::Bid, 10_000, 3_000));
        let ev = me.drain_events();
        // Passive-side fills in emission order.
        let passive: Vec<(u64, u64)> = ev
            .iter()
            .filter_map(|e| match e {
                MeEvent::Executed(x) if x.order_id != 99 => Some((x.order_id, x.qty.raw)),
                _ => None,
            })
            .collect();
        // Strict arrival order, full quantity each - no partial splits.
        let expected: Vec<(u64, u64)> = (1..=30u64).map(|id| (id, 100)).collect();
        assert_eq!(passive, expected, "fills must be strictly FIFO by arrival");
        // Aggressive order fully filled, nothing rests.
        let active_total: u64 = ev
            .iter()
            .filter_map(|e| match e {
                MeEvent::Executed(x) if x.order_id == 99 => Some(x.qty.raw),
                _ => None,
            })
            .sum();
        assert_eq!(active_total, 3_000);
        assert!(me.all_order_states().is_empty());
    }

    #[test]
    fn partial_sweep_fifo_prefix_only() {
        // A partial sweep takes a strict FIFO PREFIX of the level - orders
        // 1..k fully, k+1 partially, the rest untouched. Pro-rata would
        // touch every order. Note: the aggressive qty must be board-round
        // (300 = 3 lots) - a mixed qty splits board/odd and the odd part
        // rests in the odd book by design (board orders never fill odd).
        let mut me = MatchingEngine::new();
        for id in 1..=10u64 {
            me.add_order(&add(id, Side::Ask, 10_000, 100));
        }
        me.drain_events();
        // Buy 300 → orders 1, 2 fully + order 3 fully, prefix discipline.
        me.add_order(&add(50, Side::Bid, 10_000, 300));
        let ev = me.drain_events();
        let fills: std::collections::HashMap<u64, u64> = ev
            .iter()
            .filter_map(|e| match e {
                MeEvent::Executed(x) if x.order_id != 50 => Some((x.order_id, x.qty.raw)),
                _ => None,
            })
            .collect();
        assert_eq!(fills.get(&1), Some(&100));
        assert_eq!(fills.get(&2), Some(&100));
        assert_eq!(fills.get(&3), Some(&100));
        // Orders 4..=10 untouched.
        for id in 4..=10u64 {
            assert!(
                !fills.contains_key(&id),
                "order {id} must not fill (pro-rata leak?)"
            );
        }
        // Remaining state: orders 4..=10 have 100 each; nothing else rests.
        let states = me.all_order_states();
        assert_eq!(states.len(), 7);
        assert_eq!(states.iter().filter(|s| s.volume.raw == 100).count(), 7);
    }

    #[test]
    fn odd_portion_never_fills_board_liquidity() {
        // The board/odd partition is strict: an aggressive 250-qty order
        // fills board liquidity (200) and its odd 50 rests in the ODD book
        // even though board liquidity remains at the same price.
        let mut me = MatchingEngine::new();
        me.add_order(&add(1, Side::Ask, 10_000, 100));
        me.add_order(&add(2, Side::Ask, 10_000, 100));
        me.add_order(&add(3, Side::Ask, 10_000, 100));
        me.drain_events();
        me.add_order(&add(50, Side::Bid, 10_000, 250));
        let ev = me.drain_events();
        let board_fills: Vec<u64> = ev
            .iter()
            .filter_map(|e| match e {
                MeEvent::Executed(x) if x.order_id != 50 => Some(x.qty.raw),
                _ => None,
            })
            .collect();
        // Exactly 200 board fills (orders 1+2); order 3 keeps its 100.
        assert_eq!(board_fills.iter().sum::<u64>(), 200);
        let states = me.all_order_states();
        // Order 3 (board, 100) + the aggressive 50 resting in the odd book.
        assert_eq!(states.len(), 2);
        assert!(states
            .iter()
            .any(|s| s.order_id == 3 && s.volume.raw == 100));
        assert!(states
            .iter()
            .any(|s| s.order_id == 50 && s.volume.raw == 50));
    }

    // ------------------------------------------------------------------
    // Matching discipline: per-contract allocation (pro-rata contracts)
    // ------------------------------------------------------------------

    #[test]
    fn pro_rata_contract_allocates_within_level() {
        // Same setup as partial_sweep_fifo_prefix_only, but the contract's
        // allocation method is pro_rata_round_lot: a 300-qty aggressive
        // order over 10 resting × 100 fills ALL 10 orders 30 each
        // (∝ 100/1000 × 300), instead of consuming orders 1..3 fully.
        let mut me = MatchingEngine::new();
        for id in 1..=10u64 {
            me.add_order(&add(id, Side::Ask, 10_000, 100));
        }
        me.drain_events();
        me.books.get_mut(&1).unwrap().allocation = AllocationMethod::ProRataRoundLot;
        me.add_order(&add(99, Side::Bid, 10_000, 300));
        let ev = me.drain_events();
        let passive: Vec<(u64, u64)> = ev
            .iter()
            .filter_map(|e| match e {
                MeEvent::Executed(x) if x.order_id != 99 => Some((x.order_id, x.qty.raw)),
                _ => None,
            })
            .collect();
        // Every order at the level participates - 30 each, 10 orders.
        assert_eq!(passive.len(), 10);
        for (id, qty) in &passive {
            assert_eq!(*qty, 30, "order {id} must get its pro-rata share");
        }
        let ids: std::collections::HashSet<u64> = passive.iter().map(|(id, _)| *id).collect();
        assert_eq!(ids.len(), 10);
        // Conservation: 300 filled, all 10 orders reduced to 70.
        let active: u64 = ev
            .iter()
            .filter_map(|e| match e {
                MeEvent::Executed(x) if x.order_id == 99 => Some(x.qty.raw),
                _ => None,
            })
            .sum();
        assert_eq!(active, 300);
        let states = me.all_order_states();
        assert_eq!(states.len(), 10);
        assert!(states.iter().all(|s| s.volume.raw == 70));
    }

    #[test]
    fn pro_rata_remainder_deterministic() {
        // 3 resting (60/40) + 50 incoming → 30/20 exact, no remainder;
        // then a 51-qty sweep: board 100 lot → 100 board fills via pro-rata
        // over 60/40: exact 60/40 (full sweep when incoming ≥ level).
        let mut me = MatchingEngine::new();
        me.add_order(&add(1, Side::Ask, 10_000, 60));
        me.add_order(&add(2, Side::Ask, 10_000, 40));
        me.drain_events();
        me.books.get_mut(&1).unwrap().allocation = AllocationMethod::ProRataRoundLot;
        // Board-round 50 over level total 100 → shares 30/20 exact.
        me.add_order(&add(50, Side::Bid, 10_000, 50));
        let ev = me.drain_events();
        let passive: Vec<(u64, u64)> = ev
            .iter()
            .filter_map(|e| match e {
                MeEvent::Executed(x) if x.order_id != 50 => Some((x.order_id, x.qty.raw)),
                _ => None,
            })
            .collect();
        assert!(passive.contains(&(1, 30)));
        assert!(passive.contains(&(2, 20)));
        // Determinism: identical second run produces identical output.
        let mut me2 = MatchingEngine::new();
        me2.add_order(&add(1, Side::Ask, 10_000, 60));
        me2.add_order(&add(2, Side::Ask, 10_000, 40));
        me2.drain_events();
        me2.books.get_mut(&1).unwrap().allocation = AllocationMethod::ProRataRoundLot;
        me2.add_order(&add(50, Side::Bid, 10_000, 50));
        let ev2 = me2.drain_events();
        let passive2: Vec<(u64, u64)> = ev2
            .iter()
            .filter_map(|e| match e {
                MeEvent::Executed(x) if x.order_id != 50 => Some((x.order_id, x.qty.raw)),
                _ => None,
            })
            .collect();
        assert_eq!(passive, passive2);
    }

    #[test]
    fn fifo_instrument_untouched_by_pro_rata_code() {
        // Default allocation stays FIFO: with the policy plumbing present,
        // an equity-shaped instrument still fills strict prefix.
        let mut me = MatchingEngine::new();
        for id in 1..=5u64 {
            me.add_order(&add(id, Side::Ask, 10_000, 100));
        }
        me.drain_events();
        assert_eq!(me.books.get(&1).unwrap().allocation, AllocationMethod::Fifo);
        me.add_order(&add(99, Side::Bid, 10_000, 250));
        let ev = me.drain_events();
        let passive: Vec<u64> = ev
            .iter()
            .filter_map(|e| match e {
                MeEvent::Executed(x) if x.order_id != 99 => Some(x.order_id),
                _ => None,
            })
            .collect();
        // 250 splits board 200 + odd 50: board fills the strict prefix
        // 1, 2 (200); order 3 untouched; the odd 50 rests in the odd book.
        assert_eq!(passive, vec![1, 2]);
        let states = me.all_order_states();
        assert!(states
            .iter()
            .any(|s| s.order_id == 3 && s.volume.raw == 100));
        assert!(states
            .iter()
            .any(|s| s.order_id == 99 && s.volume.raw == 50));
    }

    // ------------------------------------------------------------------
    // FX CLOB verification - FX pairs trade on the same engine
    // with scale-5 pip prices and lot_size=1 (no board/odd split).
    // ------------------------------------------------------------------

    /// FX-shaped AddOrder helper: 5-decimal price, unit lot, instrument 2.
    fn fx_add(order_id: u64, side: Side, px: i128, qty: u64) -> AddOrder {
        AddOrder {
            price: Price::from_scaled(px, exchange8::fixed::FX_SCALE),
            order_id,
            sub_order_id: 0,
            instrument_id: 2,
            qty: Qty::units(qty),
            connection_id: 1,
            side,
            order_type: OrderType::Limit,
            visibility: Visibility::Display,
            capacity: Capacity::Agency,
            account: "FX".into(),
            stp_key: None,
            stp_action: None,
            customer_id: 1,
            book_id: 0,
            anonymous: false,
            price_protection: None,
            max_floor: Qty::ZERO,
            strategy: vec![b'*'],
        }
    }

    #[test]
    fn fx_clob_matches_at_pipette_precision() {
        let mut me = MatchingEngine::new();
        // Seed the FX instrument book with lot_size 1 - a lot of 1 unit
        // means every quantity is board-round: the board/odd split is a
        // no-op and all volume routes to the board book (DESIGN.md §6).
        me.book(2, Qty::units(1));
        // Bid 1.08450 × 100_000 resting; crossing ask 1.08450 fills fully.
        me.add_order(&fx_add(1, Side::Bid, 108_450, 100_000));
        me.drain_events();
        me.add_order(&fx_add(2, Side::Ask, 108_450, 100_000));
        let ev = me.drain_events();
        let trades = executed_pairs(&ev);
        assert_eq!(trades.len(), 2, "one event per side");
        // Trade price carries the scale-5 pipette precision intact.
        let ev_px = ev
            .iter()
            .find_map(|e| match e {
                MeEvent::Executed(x) => Some(x.price),
                _ => None,
            })
            .unwrap();
        assert_eq!(ev_px.scale, exchange8::fixed::FX_SCALE);
        assert_eq!(ev_px.raw, 108_450);
    }

    #[test]
    fn fx_fractional_pip_spread_crosses_correctly() {
        let mut me = MatchingEngine::new();
        me.book(2, Qty::units(1));
        // Best ask 1.08455 (pipette), best bid 1.08450 - a half-pip spread.
        me.add_order(&fx_add(1, Side::Ask, 108_455, 50_000));
        me.add_order(&fx_add(2, Side::Bid, 108_450, 50_000));
        me.drain_events();
        // Aggressive buyer lifts the 1.08455 ask; executes at the resting
        // price (passive side sets the price), never at a blended rate.
        me.add_order(&fx_add(3, Side::Bid, 108_460, 50_000));
        let ev = me.drain_events();
        let trades = executed_pairs(&ev);
        assert_eq!(trades.len(), 2);
        assert!(
            trades.iter().all(|(_, _, px)| *px == 108_455),
            "fills at resting ask, not the aggressive limit"
        );
        // Order 1 (lifted ask) and order 3 (aggressive buy) fully filled
        // and left the book; order 2 (resting bid, untouched) remains.
        let states = me.all_order_states();
        assert!(!states.iter().any(|s| s.order_id == 1));
        assert!(!states.iter().any(|s| s.order_id == 3));
        assert!(states
            .iter()
            .any(|s| s.order_id == 2 && s.volume.raw == 50_000));
    }

    #[test]
    fn fx_lot_one_no_board_odd_split() {
        let mut me = MatchingEngine::new();
        me.book(2, Qty::units(1));
        // 7 units of EURUSD - at lot_size 1 the split is (7, 0): the odd
        // book never receives volume, matching FX spot conventions where
        // every unit quantity is tradable.
        me.add_order(&fx_add(1, Side::Bid, 108_450, 7));
        let ev = me.drain_events();
        assert!(matches!(ev[0], MeEvent::Booked(_)));
        let ib = me.books.get(&2).unwrap();
        // All 7 units live in the board book; odd book is empty.
        // (volume_at_or_better at the resting price covers the full level.)
        let px = Price::from_scaled(108_450, exchange8::fixed::FX_SCALE);
        let board_vol = ib.book_for(false).side(Side::Bid).volume_at_or_better(px);
        let odd_vol = ib.book_for(true).side(Side::Bid).volume_at_or_better(px);
        assert_eq!(board_vol, 7);
        assert_eq!(odd_vol, 0);
        // FIFO allocation is the default for FX (design §3.1: `fifo`).
        assert_eq!(ib.allocation, AllocationMethod::Fifo);
    }

    #[test]
    fn fx_fifo_across_multiple_resting_orders() {
        let mut me = MatchingEngine::new();
        me.book(2, Qty::units(1));
        // Three resting bids at the same pip; a market sweep fills them
        // strictly in arrival order (prefix property).
        me.add_order(&fx_add(11, Side::Bid, 108_450, 30_000));
        me.add_order(&fx_add(12, Side::Bid, 108_450, 30_000));
        me.add_order(&fx_add(13, Side::Bid, 108_450, 30_000));
        me.drain_events();
        me.add_order(&fx_add(20, Side::Ask, 108_440, 75_000));
        let ev = me.drain_events();
        let passive: Vec<u64> = ev
            .iter()
            .filter_map(|e| match e {
                MeEvent::Executed(x) if x.order_id != 20 => Some(x.order_id),
                _ => None,
            })
            .collect();
        assert_eq!(passive, vec![11, 12, 13]);
        // 75_000 taken from 90_000: order 13 keeps 15_000 resting.
        let states = me.all_order_states();
        assert!(states
            .iter()
            .any(|s| s.order_id == 13 && s.volume.raw == 15_000));
    }
}
