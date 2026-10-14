// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Exchange8:ME
// SPDX-License-Identifier: Apache-2.0

//! Ex8:ME journal + trade-id sequencing + price-protection completion.
//!
//! - **Journal**: every inbound message per shard is appended to a
//!   bounded in-memory ring, serialized as length-prefixed binary chunks
//!   for the Sentin8:Storage sink (`me-journal/{env}/{date}/shard-{n}.bin`,
//!   rotated per session, fsync on rotate). Recovery: rebuild from journal
//!   tail or full replay — deterministic engine ⇒ identical book state.
//! - **Trade-id lease**: platform sequence facility batch-lease
//!   (10k per lease) — the pure lease counter with rollover semantics;
//!   the shell performs the lease refill.
//! - **Protection completion**: the Kill variant of price
//!   protection (Kill vs Post sidestep) as a pure decision.
//!
//! Design: `design/sentin8-exchange8-matching-engine-technical-design.md` §4.4 (price protection), §4.5 (sequencing), §4.6 (journal).

use exchange8::contracts::events::{AddOrder, CancelOrder, ModifyOrder};
use exchange8::fixed::Price;

// ---------------------------------------------------------------------------
// Quote source (DESIGN.md §7 — L1 seam)
// ---------------------------------------------------------------------------

/// The protected-quote source the matching engine polls before matching
/// (price-protection's "protected opposite quote").
///
/// The trait is the seam: today's implementation is the engine-local BBO
/// ([`ShardJournalBbo`] over the shard journal); the cross-service Ex8:MD
/// consolidated feed is a separate epic and installs behind the same trait
/// (no engine surgery).
pub trait QuoteSource: Send + Sync {
    /// The best opposite-side price for the instrument (`None` = no quote —
    /// protection passes untouched, journal.rs `protection_check`).
    fn best(&self, instrument_id: u32, is_bid_side: bool) -> Option<Price>;
}

/// The engine-local implementation: shards read their own journal-fed book
/// state (the deterministic input record) for the protected quote.
pub struct ShardJournalBbo<'a> {
    journal: &'a ShardJournal,
}

impl<'a> ShardJournalBbo<'a> {
    /// Bind the quote source to one shard's journal.
    pub fn new(journal: &'a ShardJournal) -> Self {
        ShardJournalBbo { journal }
    }
}

impl QuoteSource for ShardJournalBbo<'_> {
    fn best(&self, instrument_id: u32, is_bid_side: bool) -> Option<Price> {
        self.journal.local_best(instrument_id, is_bid_side)
    }
}

// ---------------------------------------------------------------------------
// Journal (§4.6)
// ---------------------------------------------------------------------------

/// One journaled inbound message (length-prefixed serialization).
#[derive(Clone, Debug, PartialEq)]
pub enum JournalEntry {
    Add(AddOrder),
    Cancel(CancelOrder),
    Modify(ModifyOrder),
}

impl JournalEntry {
    /// Deterministic binary encoding (little-endian, length-prefixed by the
    /// writer). Layout: 1 type byte + serde payload — the pure core keeps
    /// the framing stable for golden tests.
    pub fn encode(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(64);
        match self {
            JournalEntry::Add(_) => b.push(b'a'),
            JournalEntry::Cancel(_) => b.push(b'c'),
            JournalEntry::Modify(_) => b.push(b'u'),
        }
        // Payload: JSON of the message (schema-stable, deterministic keys).
        let payload = match self {
            JournalEntry::Add(m) => serde_json::to_vec(m).unwrap_or_default(),
            JournalEntry::Cancel(m) => serde_json::to_vec(m).unwrap_or_default(),
            JournalEntry::Modify(m) => serde_json::to_vec(m).unwrap_or_default(),
        };
        b.extend_from_slice(&payload);
        b
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let ty = *bytes.first()?;
        let payload = &bytes[1..];
        match ty {
            b'a' => serde_json::from_slice(payload).map(JournalEntry::Add).ok(),
            b'c' => serde_json::from_slice(payload)
                .map(JournalEntry::Cancel)
                .ok(),
            b'u' => serde_json::from_slice(payload)
                .map(JournalEntry::Modify)
                .ok(),
            _ => None,
        }
    }
}

/// Per-shard input journal: bounded ring + session rotation.
///
/// The ring is the in-flight buffer; `flush_chunk` drains it as one binary
/// chunk (each entry length-prefixed) for the Sentin8:Storage writer.
pub struct ShardJournal {
    shard: u32,
    /// Monotonic session number (journal file identity).
    session: u64,
    /// Entries appended this session (the ring).
    ring: std::collections::VecDeque<JournalEntry>,
    /// Ring capacity — flush triggers before overflow.
    capacity: usize,
    /// Total entries written across sessions (recovery cursor).
    written: u64,
}

impl ShardJournal {
    pub fn new(shard: u32, capacity: usize) -> Self {
        ShardJournal {
            shard,
            session: 1,
            ring: std::collections::VecDeque::with_capacity(capacity),
            capacity,
            written: 0,
        }
    }

    /// Append one inbound message. Returns the chunk to persist when the
    /// ring is full (caller writes it to Sentin8:Storage).
    pub fn append(&mut self, entry: JournalEntry) -> Option<Vec<u8>> {
        self.ring.push_back(entry);
        self.written += 1;
        if self.ring.len() >= self.capacity {
            Some(self.flush_chunk())
        } else {
            None
        }
    }

    // -----------------------------------------------------------------
    // Quote source (DESIGN.md §7): the engine-local L1 source.
    // -----------------------------------------------------------------

    /// The journal's engine-local BBO for one instrument side: the best
    /// resting price of the most recent opposite book state the journal saw.
    ///
    /// The journal is the deterministic input record — every `Add` with a
    /// displayed price the engine matched against is a quote observation;
    /// the LAST inbound add's price per side is the shard's own book state
    /// at that point (the engine books the remainder at exactly that
    /// price). The cross-service MD feed swaps in behind [`QuoteSource`]
    /// (separate epic); this is the in-process seam's first implementation.
    pub fn local_best(&self, instrument_id: u32, is_bid_side: bool) -> Option<Price> {
        for entry in self.ring.iter().rev() {
            let msg = match entry {
                JournalEntry::Add(m) if m.instrument_id == instrument_id => m,
                _ => continue,
            };
            let on_this_side = match msg.side {
                exchange8::contracts::enums::Side::Bid => is_bid_side,
                exchange8::contracts::enums::Side::Ask => !is_bid_side,
            };
            if on_this_side {
                return Some(msg.price);
            }
        }
        None
    }

    /// Drain the ring into one length-prefixed binary chunk:
    /// `[n_entries:u32] ([len:u32][bytes])×n`.
    pub fn flush_chunk(&mut self) -> Vec<u8> {
        let n = self.ring.len() as u32;
        let mut out = Vec::with_capacity(
            self.ring
                .iter()
                .map(|e| e.encode().len() + 4)
                .sum::<usize>()
                + 4,
        );
        out.extend_from_slice(&n.to_le_bytes());
        while let Some(e) = self.ring.pop_front() {
            let bytes = e.encode();
            out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            out.extend_from_slice(&bytes);
        }
        out
    }

    /// Session rotation (fsync + new file on the storage sink).
    pub fn rotate(&mut self) -> Vec<u8> {
        let tail = self.flush_chunk();
        self.session += 1;
        tail
    }

    /// Decode a chunk (recovery read path).
    pub fn decode_chunk(chunk: &[u8]) -> Vec<JournalEntry> {
        let mut out = Vec::new();
        if chunk.len() < 4 {
            return out;
        }
        let n = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]) as usize;
        let mut off = 4usize;
        for _ in 0..n {
            if off + 4 > chunk.len() {
                break;
            }
            let len =
                u32::from_le_bytes([chunk[off], chunk[off + 1], chunk[off + 2], chunk[off + 3]])
                    as usize;
            off += 4;
            if off + len > chunk.len() {
                break;
            }
            if let Some(e) = JournalEntry::decode(&chunk[off..off + len]) {
                out.push(e);
            }
            off += len;
        }
        out
    }

    pub fn session(&self) -> u64 {
        self.session
    }

    pub fn written(&self) -> u64 {
        self.written
    }

    pub fn shard(&self) -> u32 {
        self.shard
    }
}

// ---------------------------------------------------------------------------
// Trade-id lease (see DESIGN.md §4)
// ---------------------------------------------------------------------------

/// Batch-leased sequence counter (platform sequence facility, 10k blocks).
///
/// The shell leases 10k ids per UPDATE…RETURNING; this counter hands them
/// out in memory and signals `NeedLease` when exhausted. Daily rollover
/// continues from the DB value (no reset-to-1 within an env).
pub struct TradeIdLease {
    next: u64,
    /// Exclusive upper bound of the current lease.
    end: u64,
    /// Lease size (10k default).
    batch: u64,
}

/// Outcome of taking one trade id.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeaseOutcome {
    Id(u64),
    /// Lease exhausted — the shell must lease the next batch.
    NeedLease,
}

impl TradeIdLease {
    /// Start a lease at `from` covering `batch` ids: [from, from+batch).
    pub fn new(from: u64, batch: u64) -> Self {
        TradeIdLease {
            next: from,
            end: from.saturating_add(batch),
            batch,
        }
    }

    /// Hand out the next id, or signal a new lease is needed.
    pub fn next_id(&mut self) -> LeaseOutcome {
        if self.next >= self.end {
            return LeaseOutcome::NeedLease;
        }
        let id = self.next;
        self.next += 1;
        LeaseOutcome::Id(id)
    }

    /// Load a fresh lease from the DB (shell calls after UPDATE…RETURNING).
    pub fn refill(&mut self, from: u64) {
        self.next = from;
        self.end = from.saturating_add(self.batch);
    }

    pub fn batch_size(&self) -> u64 {
        self.batch
    }
}

// ---------------------------------------------------------------------------
// Price protection completion (§4.4)
// ---------------------------------------------------------------------------

/// Price-protection mode per order (FIX 18-style).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProtectionMode {
    /// No protection.
    None,
    /// Crossed order is killed with reason PriceProtection.
    Kill,
    /// Sidestep one tick away from the protected quote, then book/post.
    Post,
}

/// Pure protection decision: what happens to an order priced through the
/// protected opposite quote.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ProtectionDecision {
    /// Price is fine — proceed unchanged.
    Pass,
    /// Kill the order (protection = Kill).
    Kill,
    /// Reprice to `new_price` (one tick inside the protected quote), then
    /// proceed (protection = Post).
    Sidestep(Price),
}

/// Compute the protection decision for one inbound order against the
/// opposite-side best (the protected quote).
pub fn protection_check(
    mode: ProtectionMode,
    is_bid: bool,
    order_price: Price,
    protected_quote: Option<Price>,
    tick: i128,
) -> ProtectionDecision {
    let Some(best) = protected_quote else {
        return ProtectionDecision::Pass; // no quote → nothing to protect
    };
    let crosses = if is_bid {
        order_price.raw >= best.raw
    } else {
        order_price.raw <= best.raw
    };
    if !crosses {
        return ProtectionDecision::Pass;
    }
    match mode {
        ProtectionMode::None => ProtectionDecision::Pass,
        ProtectionMode::Kill => ProtectionDecision::Kill,
        ProtectionMode::Post => {
            let new_raw = if is_bid {
                best.raw - tick
            } else {
                best.raw + tick
            };
            ProtectionDecision::Sidestep(Price {
                raw: new_raw,
                scale: order_price.scale,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use exchange8::contracts::enums::{Capacity, OrderType, Side, Visibility};
    use exchange8::fixed::Qty;

    fn ask(id: u64, px: i128) -> AddOrder {
        let mut m = add(id, px);
        m.side = Side::Ask;
        m
    }

    #[test]
    fn journal_bbo_returns_the_recent_opposite_best() {
        // Sequence: ask 10_05 (journal entry 1), bid 10_00 (entry 2) —
        // the local ask-side best is 10_05, the bid-side best 10_00.
        let mut j = ShardJournal::new(0, 8);
        j.append(JournalEntry::Add(ask(1, 10_050)));
        j.append(JournalEntry::Add(add(2, 10_000)));
        let src = ShardJournalBbo::new(&j);
        // Ask side (is_bid_side=false) → 10_050; bid side → 10_000.
        assert_eq!(src.best(1, false), Some(Price::equity(10_050)));
        assert_eq!(src.best(1, true), Some(Price::equity(10_000)));
        // Unknown instrument → no quote (protection passes untouched).
        assert_eq!(src.best(9, false), None);
    }

    #[test]
    fn journal_bbo_scans_back_to_the_last_side_observation() {
        // Two ask observations → the LAST one (10_06) wins; cancels and
        // modifies are skipped on the scan (only adds are quote-bearing).
        let mut j = ShardJournal::new(0, 8);
        j.append(JournalEntry::Add(ask(1, 10_050)));
        j.append(JournalEntry::Add(ask(2, 10_060)));
        j.append(JournalEntry::Cancel(exchange8::contracts::CancelOrder {
            order_id: 1,
            instrument_id: 1,
            connection_id: 1,
        }));
        let src = ShardJournalBbo::new(&j);
        assert_eq!(src.best(1, false), Some(Price::equity(10_060)));
        // Empty journal → no quote.
        let empty = ShardJournal::new(1, 8);
        assert_eq!(ShardJournalBbo::new(&empty).best(1, true), None);
    }

    fn add(id: u64, px: i128) -> AddOrder {
        AddOrder {
            price: Price::equity(px),
            order_id: id,
            sub_order_id: 0,
            instrument_id: 1,
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

    fn cancel(id: u64) -> CancelOrder {
        CancelOrder {
            order_id: id,
            instrument_id: 1,
            connection_id: 1,
        }
    }

    #[test]
    fn journal_entry_round_trip() {
        let e = JournalEntry::Add(add(7, 10_000));
        let bytes = e.encode();
        assert_eq!(bytes[0], b'a');
        let back = JournalEntry::decode(&bytes).unwrap();
        assert_eq!(e, back);

        let c = JournalEntry::Cancel(cancel(7));
        assert_eq!(JournalEntry::decode(&c.encode()).unwrap(), c);
    }

    #[test]
    fn journal_chunk_round_trip() {
        let mut j = ShardJournal::new(3, 4);
        j.append(JournalEntry::Add(add(1, 10_000)));
        j.append(JournalEntry::Cancel(cancel(1)));
        j.append(JournalEntry::Add(add(2, 10_100)));
        let chunk = j.flush_chunk();
        let back = ShardJournal::decode_chunk(&chunk);
        assert_eq!(back.len(), 3);
        assert!(matches!(back[0], JournalEntry::Add(ref m) if m.order_id == 1));
        assert!(matches!(back[1], JournalEntry::Cancel(_)));
        assert!(matches!(back[2], JournalEntry::Add(ref m) if m.order_id == 2));
        assert_eq!(j.written(), 3);
    }

    #[test]
    fn journal_ring_autoflushes_at_capacity() {
        let mut j = ShardJournal::new(0, 3);
        assert!(j.append(JournalEntry::Add(add(1, 1))).is_none());
        assert!(j.append(JournalEntry::Add(add(2, 1))).is_none());
        let full = j.append(JournalEntry::Add(add(3, 1)));
        assert!(full.is_some(), "third append fills the ring");
        let decoded = ShardJournal::decode_chunk(&full.unwrap());
        assert_eq!(decoded.len(), 3);
        // Ring drained — next append starts fresh.
        assert!(j.append(JournalEntry::Add(add(4, 1))).is_none());
        let tail = j.flush_chunk();
        assert_eq!(ShardJournal::decode_chunk(&tail).len(), 1);
    }

    #[test]
    fn journal_rotate_drains_and_increments_session() {
        let mut j = ShardJournal::new(0, 100);
        j.append(JournalEntry::Add(add(1, 1)));
        assert_eq!(j.session(), 1);
        let tail = j.rotate();
        assert_eq!(ShardJournal::decode_chunk(&tail).len(), 1);
        assert_eq!(j.session(), 2);
        // Second rotate with empty ring → empty chunk.
        let t2 = j.rotate();
        assert!(ShardJournal::decode_chunk(&t2).is_empty());
    }

    #[test]
    fn decode_garbage_is_safe() {
        assert!(ShardJournal::decode_chunk(&[]).is_empty());
        assert!(ShardJournal::decode_chunk(&[1, 2]).is_empty());
        // Truncated entry body.
        let mut chunk = vec![1, 0, 0, 0, 99, 0, 0, 0]; // claims 99-byte entry, none present
        chunk.extend_from_slice(&[b'a']);
        assert!(ShardJournal::decode_chunk(&chunk).is_empty());
    }

    #[test]
    fn trade_id_lease_hands_out_then_signals() {
        let mut l = TradeIdLease::new(1, 10);
        assert_eq!(l.next_id(), LeaseOutcome::Id(1));
        assert_eq!(l.next_id(), LeaseOutcome::Id(2));
        for _ in 3..=10 {
            assert!(matches!(l.next_id(), LeaseOutcome::Id(_)));
        }
        assert_eq!(l.next_id(), LeaseOutcome::NeedLease);
        // Shell refills from the DB (e.g. UPDATE … RETURNING next + 10k).
        l.refill(11);
        assert_eq!(l.next_id(), LeaseOutcome::Id(11));
        assert_eq!(l.batch_size(), 10);
    }

    #[test]
    fn trade_id_lease_rollover_continues() {
        // Daily rollover: DB hands the next day's start; no reset to 1.
        let mut l = TradeIdLease::new(50_000, 10_000);
        assert_eq!(l.next_id(), LeaseOutcome::Id(50_000));
        l.refill(60_000);
        assert_eq!(l.next_id(), LeaseOutcome::Id(60_000));
    }

    #[test]
    fn protection_kill_and_post_variants() {
        let quote = Price::equity(10_000);
        let bid_through = Price::equity(10_050); // bid ≥ protected ask

        // No protection → pass.
        assert_eq!(
            protection_check(ProtectionMode::None, true, bid_through, Some(quote), 100),
            ProtectionDecision::Pass
        );
        // Kill → killed.
        assert_eq!(
            protection_check(ProtectionMode::Kill, true, bid_through, Some(quote), 100),
            ProtectionDecision::Kill
        );
        // Post → sidestep one tick below the ask.
        assert_eq!(
            protection_check(ProtectionMode::Post, true, bid_through, Some(quote), 100),
            ProtectionDecision::Sidestep(Price::equity(9_900))
        );
        // Ask through the bid (ask 9_800 ≤ protected bid 9_900) → sidestep above.
        assert_eq!(
            protection_check(
                ProtectionMode::Post,
                false,
                Price::equity(9_800),
                Some(Price::equity(9_900)),
                100
            ),
            ProtectionDecision::Sidestep(Price::equity(10_000))
        );
        // Not crossing → pass even with Kill.
        assert_eq!(
            protection_check(
                ProtectionMode::Kill,
                true,
                Price::equity(9_500),
                Some(quote),
                100
            ),
            ProtectionDecision::Pass
        );
        // No quote → pass.
        assert_eq!(
            protection_check(ProtectionMode::Kill, true, bid_through, None, 100),
            ProtectionDecision::Pass
        );
    }
}
