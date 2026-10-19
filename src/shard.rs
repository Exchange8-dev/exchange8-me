// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Exchange8:ME
// SPDX-License-Identifier: Apache-2.0

//! Exchange8:ME shard dispatcher + instrument metadata pre-load (shell core).
//!
//! Design §4.1: every event for instrument *i* is handled by exactly one
//! shard - `instrument_id % N_SHARDS` (N = 64 default) - so book mutation
//! needs no locks (one tokio task per shard, bounded mpsc in, single
//! consumer).
//!
//! Design §4.2 (hydration constraint): instrument metadata - `lot_size`,
//! `allocation_method` - loads from the contract master at **startup and
//! on registry refresh**, never lazily at first order. A book born before
//! its contract row loads would freeze the default policy (`fifo`,
//! lot 100) for that instrument's lifetime.
//!
//! This module is the deterministic core of the dispatcher: the shard map,
//! the metadata registry (ArcSwap-style snapshot swap), and the per-shard
//! order of operations. The shell hosts the tokio tasks around it.
//!
//! Design: `design/sentin8-exchange8-matching-engine-technical-design.md` §4.1-4.2.

use std::collections::HashMap;
use std::sync::Arc;

use exchange8::book::AllocationMethod;
use exchange8::contracts::events::{
    AddOrder, CancelOrder, InstrumentStatusMsg, MarketStatusMsg, ModifyOrder, OrderStateMsg,
};
use exchange8::fixed::Qty;

/// Default shard count (design §9 sizing: 64 shards).
pub const DEFAULT_SHARDS: usize = 64;

/// Per-instrument metadata loaded from the contract master (§4.2).
#[derive(Clone, Debug, PartialEq)]
pub struct InstrumentMeta {
    pub instrument_id: u32,
    pub lot_size: Qty,
    pub allocation: AllocationMethod,
}

impl Default for InstrumentMeta {
    fn default() -> Self {
        InstrumentMeta {
            instrument_id: 0,
            lot_size: Qty::units(100),
            allocation: AllocationMethod::Fifo,
        }
    }
}

/// The immutable metadata snapshot (ArcSwap payload): startup load +
/// event-driven refresh swap whole snapshots - readers never see torn
/// state, and a book created after the swap sees the new policy.
#[derive(Clone, Debug, Default)]
pub struct InstrumentRegistry {
    instruments: HashMap<u32, InstrumentMeta>,
}

impl InstrumentRegistry {
    pub fn from_rows(rows: Vec<InstrumentMeta>) -> Self {
        let mut instruments = HashMap::new();
        for r in rows {
            instruments.insert(r.instrument_id, r);
        }
        InstrumentRegistry { instruments }
    }

    /// Resolve metadata; unknown instruments get the documented default
    /// (lot 100, fifo) - the same default the engine's lazy path would
    /// produce, kept consistent for journal replay of pre-registry orders.
    pub fn get(&self, instrument_id: u32) -> InstrumentMeta {
        self.instruments
            .get(&instrument_id)
            .cloned()
            .unwrap_or_default()
    }

    pub fn len(&self) -> usize {
        self.instruments.len()
    }

    pub fn is_empty(&self) -> bool {
        self.instruments.is_empty()
    }
}

/// The shared registry handle: `snapshot()` is the hot-path read.
pub struct RegistryHandle {
    current: Arc<InstrumentRegistry>,
}

impl RegistryHandle {
    /// Startup (SOD) load - the shell projects the contract master rows.
    pub fn from_sod(registry: InstrumentRegistry) -> Self {
        RegistryHandle {
            current: Arc::new(registry),
        }
    }

    /// Hot path: one Arc clone.
    pub fn snapshot(&self) -> Arc<InstrumentRegistry> {
        self.current.clone()
    }

    /// Event-driven refresh: copy → mutate → swap (atomic for readers).
    pub fn refresh(&mut self, mutate: impl FnOnce(&mut InstrumentRegistry)) {
        let mut next = (*self.current).clone();
        mutate(&mut next);
        self.current = Arc::new(next);
    }
}

/// One inbound ME message (the union the dispatcher routes).
#[derive(Clone, Debug, PartialEq)]
pub enum MeInbound {
    Add(AddOrder),
    Cancel(CancelOrder),
    Modify(ModifyOrder),
    InstrumentStatus(InstrumentStatusMsg),
    MarketStatus(MarketStatusMsg),
    OrderState(OrderStateMsg),
}

impl MeInbound {
    /// The shard key: the instrument the message touches.
    pub fn instrument_id(&self) -> u32 {
        match self {
            MeInbound::Add(m) => m.instrument_id,
            MeInbound::Cancel(m) => m.instrument_id,
            MeInbound::Modify(m) => m.instrument_id,
            MeInbound::InstrumentStatus(m) => m.instrument_id,
            MeInbound::MarketStatus(_) => 0, // market-wide: shard 0 fans out
            MeInbound::OrderState(m) => m.instrument_id,
        }
    }
}

/// The shard assignment: `instrument_id % N_SHARDS`
#[inline]
pub fn shard_of(instrument_id: u32, shards: usize) -> usize {
    (instrument_id as usize) % shards.max(1)
}

/// Dispatcher accounting for a batch of messages: per-shard queues the
/// shell materializes as bounded mpsecs. Pure core so the assignment is
/// unit-testable without tokio.
pub fn dispatch_batch(msgs: &[MeInbound], shards: usize) -> Vec<Vec<MeInbound>> {
    let mut out: Vec<Vec<MeInbound>> = vec![Vec::new(); shards.max(1)];
    for m in msgs {
        out[shard_of(m.instrument_id(), shards)].push(m.clone());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use exchange8::contracts::enums::{Capacity, OrderType, Side, Visibility};

    fn add(id: u64, inst: u32) -> AddOrder {
        AddOrder {
            price: exchange8::fixed::Price::equity(10_000),
            order_id: id,
            sub_order_id: 0,
            instrument_id: inst,
            qty: Qty::units(100),
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
            max_floor: Qty::ZERO,
            strategy: vec![b'*'],
        }
    }

    #[test]
    fn shard_assignment_is_stable_and_balanced() {
        // Same instrument → same shard, always.
        assert_eq!(shard_of(7, 64), shard_of(7, 64));
        // instrument i and i+64 share a shard (mod N).
        assert_eq!(shard_of(3, 64), shard_of(67, 64));
        // Distinct residues → distinct shards.
        assert_ne!(shard_of(1, 64), shard_of(2, 64));
        // Degenerate shard count 1 is safe.
        assert_eq!(shard_of(999, 1), 0);
    }

    #[test]
    fn dispatch_routes_every_message_exactly_once() {
        let msgs: Vec<MeInbound> = (1..=100u64)
            .map(|id| MeInbound::Add(add(id, (id % 5) as u32)))
            .collect();
        let queues = dispatch_batch(&msgs, DEFAULT_SHARDS);
        // Every message lands in exactly one queue.
        let total: usize = queues.iter().map(|q| q.len()).sum();
        assert_eq!(total, msgs.len());
        // Each queue only holds its own shard's instruments.
        for (i, q) in queues.iter().enumerate() {
            for m in q {
                assert_eq!(shard_of(m.instrument_id(), DEFAULT_SHARDS), i);
            }
        }
    }

    #[test]
    fn registry_pre_load_hydrates_policies() {
        // The §4.2 constraint: a pro-rata commodity future's policy must be
        // visible through the registry BEFORE any order for it arrives.
        let rows = vec![
            InstrumentMeta {
                instrument_id: 1,
                lot_size: Qty::units(100),
                allocation: AllocationMethod::Fifo,
            },
            InstrumentMeta {
                instrument_id: 2,
                lot_size: Qty::units(50),
                allocation: AllocationMethod::ProRataRoundLot,
            },
        ];
        let handle = RegistryHandle::from_sod(InstrumentRegistry::from_rows(rows));
        let snap = handle.snapshot();
        assert_eq!(snap.len(), 2);
        assert_eq!(snap.get(2).allocation, AllocationMethod::ProRataRoundLot);
        assert_eq!(snap.get(2).lot_size, Qty::units(50));
        // Unknown instrument → documented default (lot 100, fifo).
        let d = snap.get(99);
        assert_eq!(d.lot_size, Qty::units(100));
        assert_eq!(d.allocation, AllocationMethod::Fifo);
    }

    #[test]
    fn registry_refresh_swaps_atomically() {
        let mut handle =
            RegistryHandle::from_sod(InstrumentRegistry::from_rows(vec![InstrumentMeta {
                instrument_id: 1,
                lot_size: Qty::units(100),
                allocation: AllocationMethod::Fifo,
            }]));
        let old = handle.snapshot();
        // Ops flips instrument 1 to pro-rata (contract master update event).
        handle.refresh(|r| {
            r.instruments.get_mut(&1).unwrap().allocation = AllocationMethod::ProRataRoundLot;
        });
        let new = handle.snapshot();
        assert_eq!(new.get(1).allocation, AllocationMethod::ProRataRoundLot);
        // Old snapshot untouched (readers keep consistent views).
        assert_eq!(old.get(1).allocation, AllocationMethod::Fifo);
        assert!(!Arc::ptr_eq(&old, &new));
    }

    #[test]
    fn market_status_routes_to_fanout_shard() {
        let m = MeInbound::MarketStatus(MarketStatusMsg {
            book_id: 0,
            status: exchange8::contracts::enums::MarketStatus::Halt,
        });
        assert_eq!(m.instrument_id(), 0);
        assert_eq!(shard_of(m.instrument_id(), 64), 0);
    }
}
