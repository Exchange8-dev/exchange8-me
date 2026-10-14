# BUSINESS.md: Ex8:ME — Business Requirements & Functionality

> Audience: product owners, business analysts, and anyone evaluating what
> Ex8:ME **does for the business**. For architecture read `README.md`;
> for wire-level detail read `DESIGN.md`.

## 1. Business Purpose

- Operate the exchange's central order book: match buy and sell interest
  fairly, transparently, and deterministically for every instrument the
  venue lists — equities, futures, FX, crypto-shaped products.
- Guarantee fill fairness under audit: price-time priority (with
  contract-defined pro-rata variants), strictly reproducible outcomes, and a
  complete event trail of every order booking, execution, kill, and
  rejection.
- Keep the market open and orderly: instrument halts, quotation-only
  periods, and call auctions (opening / closing / volatility / IPO / reopen
  / halt) are executed by the engine so price discovery survives stress and
  listing events.
- Publish the truth of the market in real time: every matched quantity is
  emitted as paired liquidity events that downstream market data, clearing,
  and surveillance consume.

## 2. Business Scope

### What the service provides

| Capability | Description |
|---|---|
| Continuous order matching | Price-time CLOB matching on in-memory books, all order types the venue supports (limit, IOC, FOK, post-only, bypass, iceberg display) |
| Contract-defined fill discipline | FIFO prefix on equities-style contracts; pro-rata (round-lot, FIFO-remainder variants) on futures/hybrid contracts |
| Broker preferencing | Two-pass fill: same-customer liquidity first (even at worse price), then the open book |
| Board / odd-lot separation | Whole-lot board book and odd-lot book never cross; mixed quantities split automatically |
| Iceberg execution | Hidden reserve with visible display slice; automatic reload; time priority resets on reload |
| Self-trade prevention | Kill-active / kill-passive / kill-both / decrement actions on matching self-trade keys; disabled for fill-or-kill |
| Price protection | Pre-match sidestep one tick from the protected opposite quote (post or kill variants) |
| Market state enforcement | Instrument halt (resting sweep + rejection), quotation-only (plain display limits book without matching) |
| Call auctions | Call period with order accumulation, indicative price/volume/imbalance publication, maximum-volume uncross with deterministic tie-breaks |
| Order management | Cancel, modify/replace (re-key; modify loses time priority), erase (silent), cancel-on-disconnect |
| Recovery & reconciliation | All-orders state reporting, silent erase, journaled input replay to an identical book |
| Trade identity | Platform-leased, env-unique trade ids; paired active/passive events per fill |
| Realtime event publication | MemBus `me.exec` stream for realtime adopters; durable event delivery via outbox → Bus for clearing, data, and downstream triggers |
| Admin surfaces | Shield policy administration (policy sets, relations, attributes) and declared RFW admin pages for engine and auctions |

### What the service does NOT own (business boundary)

| Concern | Owning service |
|---|---|
| Client connectivity & session protocols (FIX, SOUP, order entry UX) | Ex8:Gateway |
| Pre-trade risk checks (position, credit, fat-finger limits) | Ex8:Risk |
| Order lifecycle above the fill (parent orders, allocation, drop copy) | Ex8:OMS |
| Smart order routing decisions | Ex8:SOR |
| Market data book assembly / feed publication (ITCH/MOLD) | Ex8:MD |
| Reference data: instruments, contracts, tick rules, books, calendars | Ex8:RefData |
| Trade persistence, clearing, netting | Ex8:Clearing |
| Settlement of cleared trades | Ex8:Settlement |
| Margin calculation | Ex8:Margin |
| Market surveillance | Ex8:Surveillance |
| Identity, tokens, authorization decisions engine | Sentin8:Auth / Shield (declared sets) |
| Event durability & fan-out infrastructure | Sentin8:Bus / Sentin8:MemBus |
| Audit archival | Sentin8:Sys |

## 3. Business Capabilities

### Continuous matching (order handling)

| Capability | Business rule |
|---|---|
| Best execution | Incoming orders execute against the best available price on the opposite side; execution price is always the resting order's price — never a blended rate |
| Price-time priority (default) | On FIFO contracts, an aggressive order consumes a strict prefix of each price level: earliest orders in full, at most one partially, later orders untouched |
| Pro-rata allocation (contract-specific) | Futures/hybrid contracts distribute fills proportionally to resting size, with a deterministic indivisible remainder — no order at the level is skipped |
| Broker preferencing | When the instrument prefers brokers, the incoming order first fills resting orders of its own customer, even at inferior prices, before the open book |
| Immediate-or-cancel | Any unfilled remainder is killed immediately; partial fills stand |
| Fill-or-kill | The order fills entirely against available liquidity or does not trade at all (pre-check of opposite volume; no partials) |
| Post-only | An order that would immediately match is killed as crossed; otherwise it rests without ever taking liquidity |
| Bypass | Matches only against visible liquidity, ignoring iceberg-hidden volume; remainder killed |
| Iceberg orders | Only the displayed slice is visible and matchable; each auto-reload re-enters at the tail of the queue (loses time priority) with an incremented sub-order count |
| Mixed-lot handling | Quantities beyond whole lots split: the board portion matches board liquidity, the odd remainder rests in the odd book; board orders never fill odd liquidity |

### Market state & protection

| Capability | Business rule |
|---|---|
| Instrument halt | Halting an instrument kills every resting order (reason: halted); new orders are rejected until the state changes |
| Quotation-only period | Only plain display limit orders are accepted and they rest without matching; all other order types are rejected |
| Price protection | A limit that would price through the protected opposite quote is either killed or sidestepped one tick toward its side; the reprice itself is published for audit |
| Self-trade prevention | Orders whose self-trade key matches a resting order apply the active order's policy: kill the active remainder, remove the passive order, kill both, or decrement; fill-or-kill orders bypass STP |
| Halts & market-wide events | Market-wide status changes route through the fan-out shard for book-wide treatment; reopen resumes continuous trading |

### Auctions

| Capability | Business rule |
|---|---|
| Auction types | Opening, closing, volatility, IPO, reopen, and halt-triggered auctions (the venue's order/venue vocabulary) |
| Call period | Orders accumulate (auction limits, market-on-open/close, market orders); participants may cancel during the call |
| Indicative publication | During the call the engine continuously derives indicative price, matched volume, and signed imbalance so participants can react |
| Uncrossing | A single clearing price maximizes matched volume; ties resolve by smallest absolute imbalance, then closest to the reference (previous close / last trade) price |
| Uncross integrity | Executions clear at the single auction price — no partial-price fills; the residual imbalance is published |

### Order administration & recovery

| Capability | Business rule |
|---|---|
| Cancel | Resting order removed and owner notified; canceling an unknown order is an explicit rejection, never silence |
| Modify/replace | Replaces price/quantity/floor under a new order id and always loses time priority; reducing to zero is a kill |
| Erase | Removes a ghost/inconsistent order without notifying the originator (operator-reconciliation primitive) |
| Cancel-on-disconnect | A dropped session's working orders can be swept by the session supervisor rather than left resting |
| State reconciliation | The engine reports every resting order's remaining volume on request; upstream decides erase vs cancel for mismatches |
| Crash recovery | The journaled input is replayed deterministically to rebuild identical books; trade numbering resumes without reuse |

## 4. Business Events (Outbound)

Emitted lifecycle/trade events (consumed in realtime via the `me.exec`
MemBus stream class; durably delivered via `me.outbox` → Sentin8:Bus —
every capability maps to one of these exact event strings, batched into
the outbox in groups of 1000 events or 5 ms):

| Event | When it fires |
|---|---|
| `exchange8:me:order:booked` | Order (or remainder) rests on the book |
| `exchange8:me:trade:executed` | Every fill side (paired by the shared trade id); active liquidity-removed, passive liquidity-added |
| `exchange8:me:order:killed` | Cancel, modify-to-zero, IOC/FOK/Bypass remainder, post-only crossed, self-trade action, halt sweep, connection cancel |
| `exchange8:me:order:modified` | Order replaced (new id, new price/qty/floor) |
| `exchange8:me:order:rejected` | Halted instrument, quotation-only violation, unknown-order cancel/modify |
| `exchange8:me:order:repriced` | Price-protection sidestep applied before matching |
| `exchange8:me:auction:uncrossed` (`me.auction` family) | Call ends: durable session completes — clearing price, matched quantity, residual imbalance, participation snapshot |

Inbound events consumed: contract-master / instrument metadata updates
(registry refresh), Shield policy-set updates (policy cache), market-wide
status distribution.

## 5. Business Rules & Invariants

1. Execution price is always the resting order's price — the passive side
   sets the price; aggressors never blend.
2. Both sides of one fill share a single trade id; every execution produces
   exactly one event per side.
3. On FIFO instruments the fill order within a price level is exactly
   arrival order; a partial sweep takes a strict prefix and touches nothing
   else.
4. Pro-rata instruments fill every resting order at the level in
   proportion to size; allocation is deterministic run-to-run identical.
5. Board liquidity and odd-lot liquidity never match each other, in either
   direction; lot-size-1 contracts (FX/crypto) have no odd remainder by
   construction.
6. An iceberg's hidden reserve is never visible or matchable; after each
   visible slice trades out, the next slice re-enters at the queue tail.
7. Self-trades never produce fills: matching self-trade pairs are killed or
   decremented per the active order's policy, never executed.
8. Fill-or-kill either fills in full or leaves the book untouched
   (including no STP interaction); immediate-or-cancel may partially fill.
9. Post-only orders never take liquidity: they either rest or are killed as
   crossed.
10. Halting an instrument removes every resting order first; while halted
    (or quotation-only beyond display limits) new orders are rejected.
11. Every accepted order decision is journaled before its effects are
    observable downstream; replaying the journal reproduces the identical
    book and trade stream.
12. Trade ids are unique per environment (platform-sequence leases, no
    reuse); order ids are assigned upstream and treated as opaque.
13. No tenant (environment) can ever see or influence another environment's
    book: row-level security is forced on all durable tables and the books
    themselves are per-env memory structures.
14. Modify always loses time priority (re-keyed entry); a pure quantity
    reduction below zero is impossible, and full reduction kills the order.
15. Auction execution is single-price: every participant trades at the
    uncross price; imbalance and matched volume are published during the
    call.
16. Authorization is fail-closed: without a matching permit, orders are
    rejected; shadow policies never block trading; tenant forbids always
    override platform permits.
17. A market-data or bus outage never stops matching: realtime publication
    degrades with a warning while durable delivery retries until it
    succeeds (audit is delayed, never lost).

## 6. Target-User Roles & Use Cases

| Persona | Role | What they do with Ex8:ME |
|---|---|---|
| Retail / institutional trader | Order submitter via broker/OMS | Submits limit/IOC/FOK/post-only/iceberg orders through Gateway/OMS; receives fills, kills, rejects |
| Sell-side broker / market maker | Quoting member | Rests iceberg and post-only quotes; benefits from preferencing and STP policies on their own flow |
| Exchange operations | Market operator | Halts/resumes instruments, moves state to quotation-only, triggers auctions, erases ghost orders during reconciliation |
| Listing / IPO sponsor | Issuer-side | Uses IPO and reopening auctions for fair price discovery on new or reopened listings |
| Market data & venue feeds | Consumer | Builds the public book from the event stream; consumes auction indicatives |
| Clearing & settlement | Consumer | Persists and clears the authoritative trade stream |
| Surveillance & compliance | Consumer | Reconstructs every order lifecycle and fill decision from the event trail |
| Venue administrator | Shield admin | Manages the engine's policy sets (enforce/shadow), relations, attributes via the admin API |
| Platform engineering | Operator | Rolling upgrades with drain, recovery from journal replay, outbox/audit delivery monitoring |

## 7. KPIs / Success Criteria

- **Matching latency**: P50 well inside the sub-100-microsecond envelope
  budget in the master design; no allocation on the fill path.
- **Throughput**: aggregate order flow of tens of thousands of orders per
  second across 1k instruments and 64 shards without queue saturation.
- **Fairness fidelity**: zero FIFO-prefix or pro-rata violations in the
  property-test suite (CI-enforced); identical replay output for every
  recovery.
- **Correctness under audit**: complete event trail — every accepted input
  journaled; every fill paired per side with a shared trade id and passive
  price.
- **Availability**: matches continue through market-data and bus outages;
  rolling upgrades complete without dropped resting orders (drain +
  journal + replay).
- **Isolation**: zero cross-environment information flow (RLS matrix
  green in every suite run).
- **Market quality**: auctions publish indicative imbalance during calls
  and clear at the maximum-volume price with deterministic tie-breaks.

## 8. Dependencies & Reliance

- **Ex8:Gateway / Ex8:OMS / Ex8:SOR** — the only order sources; ME treats
  their order ids as opaque and trusts their session and framing.
- **Ex8:Risk** — pre-trade authorization must have run upstream; ME is not
  a risk gate.
- **Ex8:RefData** — instrument metadata (lot size, allocation policy,
  status) pre-loaded into the registry; cross-schema read of
  `refdata.security` is the only grant outside the service's schema.
- **Ex8:MD** — consumes the event stream for book/feed assembly; ME mirrors
  nothing itself.
- **Ex8:Clearing** — the persistence sink for trades; ME keeps only the
  in-flight lifecycle.
- **Sentin8:Auth** — all token issuance/verification; M2M workloads
  (`exchange8:me`/`me` streams, `me-audit`, `exchange8-me-tls`) and
  cert-bound defaults.
- **Sentin8:Policy (Shield)** — the only authorization gate; policy-set
  cache shared between admin and runtime paths.
- **Sentin8:Bus / Sentin8:MemBus** — durable event delivery (outbox relay)
  and realtime publication; both degrade without blocking matching.
- **Sentin8:Sys** — audit ingest and the TLS hostname resolution owner API.
- **Sentin8:DB (PostgreSQL 18, db `exchange8`, schema `me`)** — auction
  sessions, outbox, audit journal; never touched without a tenant context.
- **Platform shell (`sentin8::shell`)** — registration, pool, health, drain,
  allocator; and `sentin8-tls` for in-process post-quantum TLS.

## 9. Acceptance Criteria

1. An aggressive limit order fills strictly in price-time order on a FIFO
   instrument; each fill emits paired events with one trade id per fill and
   the resting price.
2. A pro-rata contract distributes a sweep over the whole level
   proportionally with a deterministic remainder; equity FIFO behavior is
   unchanged by that code path.
3. IOC/FOK/post-only/bypass semantics match the business rules above,
   including FOK's no-partial-trade guarantee and post-only's
   crossed-kill.
4. Icebergs reload hidden slices, lose time priority on reload, and reveal
   nothing beyond `max_floor`.
5. STP policies (kill-active/passive/both/decrement) prevent any self-trade
   fill; FOK orders skip STP.
6. Mixed quantity 250 (lot 100) fills only board liquidity and rests the
   odd 50; board and odd never cross.
7. Instrument halt sweeps all resting orders and rejects new order types;
   quotation-only accepts only plain display limits to rest.
8. Auctions accumulate during the call, publish indicative price/volume/
   imbalance, and uncross at the maximum-matched-volume price with
   deterministic tie-breaks.
9. Kill/recovery: killing the process at any point and replaying the
   journal yields the identical book state and trade id frontier.
10. Cancel/modify/erase behave per the rules above (re-keyed modify, kill
    on zero, silent erase, not-found rejection).
11. Every durable table is RLS-forced with provable env isolation (no
    context → zero rows; cross-env → zero rows; contextless insert →
    rejected).
12. Authorization is enforced solely by Shield's seeded scope mirror:
    fail-closed without sets, shadow never blocks, tenant forbids win,
    M2M parity holds.
13. Events persist durably before delivery: outbox rows inserted in the
    domain transaction, relayed idempotently to Bus, retried indefinitely
    on outage, DLQ-replayable on failure.
14. Admin changes ride the audit journal to Sys with before/after states;
    Sys outages delay rather than lose audit records.
15. Rolling instance upgrades complete with graceful drain and no stale
    registry entries or lost resting orders.

## 10. Open Business Questions

- Exact public event names/fields for the outbound trade and lifecycle
  stream (the vocabulary `exchange8:{service}:{resource}:{operation}` is
  fixed; the per-event payload contract for clearing/market-data consumers
  is being finalized).
- Publicly documented fee/charge flags (midpoint charge, principal/wash
  trade markers) are carried by the event format but their business
  calculation rules live with downstream consumers — to be codified with
  clearing.
- Auction scheduling policy for the venue (which calendar events trigger
  which auction types per market, and who may initiate intraday auctions)
  is owned by the market-operations workflow pending admin-surface
  integration.
- Randomized uncross window length and its anti-gaming parameters are an
  exchange-policy decision to be set per market before auction go-live.
- Anonymous-vs-attributed trade disclosure rules by participant class: the
  engine carries the anonymity flag; the disclosure policy per product is
  business-owned and pending final rule text.

---

**Design attribution:** exchange8:me and its underlying concepts were designed by Hamid Jahanzad <Hamid@Jahanzad.com>.
