# Polymarket Sports Odds — corrected prototype

Read-only Node.js dashboard. It discovers an event by slug through Gamma, preserves actual outcomes/token IDs, gets CLOB book snapshots, and streams public Market WebSocket updates.

## Run
Requires Node.js 20+ and internet access.
```bash
npm install
npm start
```
Open http://localhost:3000. The input accepts `atp-nardi-holmgre-2026-10-13` or a full Polymarket sports URL. Load the WTA event separately with `wta-pohanko-wan-2026-10-13`. Each loaded event is rendered separately; do not merge their books.

## Display semantics
- Prices shown are **decimal odds** (`1 / CLOB price`), to 3 decimal places.
- “Ask odds (buy)” derives from the lowest ask; “Bid odds (sell)” derives from the highest bid.
- Size is the top level's share quantity, not guaranteed fill size for an arbitrary stake.
- Actual market outcome labels are preserved. These columns are not automatically labelled BACK/LAY because Polymarket outcome tokens and exchange-style lay semantics are not universally equivalent.
- Market metadata volume/liquidity are Gamma-reported fields, not executable size at the displayed price.

## Data flow
- Gamma API: event/market metadata and outcome-to-token mapping.
- CLOB REST `/book`: initial snapshot/fallback.
- Public CLOB Market WebSocket: snapshots and level changes.
- Express: browser-facing proxy and WebSocket relay.

No wallet, API key, or private credentials are needed for these public read-only endpoints.

## Known limitations
This is still a prototype, not a production-grade exchange engine. The browser applies `book` snapshots and `price_change` level replacements, removes levels with size zero, and marks books unsynced after disconnect. Snapshot-vs-stream timing can still require stricter reconciliation in a production deployment; stale state is not a substitute for a guaranteed gap-free feed. There is no stake-specific depth-walking average price, fees calculation, sports-score feed, or automatic all-sports event discovery. Only markets returned for the selected Gamma event are displayed. Use live order books as indicative data and independently verify before making decisions.

## Official docs
- https://gamma-api.polymarket.com/docs
- https://docs.polymarket.com/market-data/realtime-data
