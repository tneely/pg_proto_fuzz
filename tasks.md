# pg_proto_fuzz — Tasks

Ordered to reach a working end-to-end fuzzer as early as possible, then layer on
sophistication. Each task produces something testable.

---

## Phase 1: Core types

Everything else depends on these. No pg_stream usage yet — pure data definitions.

### 1.1 FrontendOp and supporting types (`op.rs`)

Define:
- `FrontendOp` enum (all variants from design)
- `StmtName` — newtype over `Option<String>` (None = unnamed `""`)
- `PortalName` — same pattern
- `Param` — enum for bindable parameter values (null, i32, text, bytes)
- Name pool constants: `STMT_NAMES`, `PORTAL_NAMES`
- `Display` impl for `FrontendOp` (human-readable, used in reports)

### 1.2 SQL template registry (`template.rs`)

Define:
- `Affinity` enum (Simple, Extended, Any)
- `SqlEntry` struct with all fields from design (sql, affinity, requires, param_count, setup)
- `fn all_templates() -> &'static [SqlEntry]` returning the initial registry
- `fn enabled_templates(profile: &FuzzProfile) -> Vec<&SqlEntry>` — filter by tags

### 1.3 FuzzProfile (`profile.rs`)

Define:
- `FuzzProfile` struct (HashSet of feature tag strings)
- `FuzzProfile::minimal()`, `::standard()`, `::full()` presets
- `FuzzProfile::is_enabled(&self, tag: &str) -> bool`
- `FuzzProfile::enable(&mut self, tag: &str)` / `disable(&mut self, tag: &str)`

---

## Phase 2: Encoding and single-connection replay

Get bytes on the wire. This is where pg_stream comes in.

### 2.1 Encode FrontendOp to pg_stream wire messages (`runner.rs`)

Write a function that takes a `&FrontendOp` and a `&mut PgConnection` and calls the
appropriate `PgProtocol` methods:

- `Query` → `conn.query(sql)`
- `Parse` → `conn.parse(name).query(sql).finish(param_oids)` (via `ParseBuilder`)
- `Bind` → `conn.bind(portal).statement(stmt).finish(params)` (via `BindBuilder`)
- `DescribeStatement` → `conn.describe_statement(name)`
- `DescribePortal` → `conn.describe_portal(name)`
- `Execute` → `conn.execute(portal, max_rows)`
- `CloseStatement` → `conn.close_statement(name)`
- `ClosePortal` → `conn.close_portal(name)`
- `Sync` → `conn.sync()`
- `Flush` → `conn.flush_msg()`
- `CopyData` → `conn.copy_data(data)`
- `CopyDone` → `conn.copy_done()`
- `CopyFail` → `conn.copy_fail(msg)`
- `Terminate` → `conn.terminate()`

This is the critical translation layer. Get the pg_stream API calls exactly right.

### 2.2 Response collection

Write a function that reads `PgMessage`s from a `PgConnection` until a stopping condition:
- Accumulate messages into a `Vec<PgMessage>` (or our own `ResponseEvent` wrapper)
- Stop on `ReadyForQuery` (return collected messages)
- Stop on timeout (return what we have + a `Timeout` event)
- Stop on connection close / error (return what we have + a `Disconnected` event)

This needs to handle the fact that some ops don't produce a `ReadyForQuery` (e.g.,
Flush produces responses but no ReadyForQuery). The collection logic needs to understand
flush points: after each TCP flush, collect whatever comes back until we've drained
responses or hit the timeout.

### 2.3 Single-connection smoke test

Wire up a minimal `main.rs` that:
1. Connects to a local Postgres via `pg_stream::ConnectionBuilder`
2. Sends a hardcoded sequence (e.g., `Parse("", "SELECT 1") → Bind("", "") → Execute("", 0) → Sync`)
3. Collects and prints every `PgMessage` received
4. Disconnects

This validates that encoding and response collection work before we add the second
connection. Run it against a real Postgres to verify.

---

## Phase 3: Dual-connection comparison

### 3.1 Runner: dual-connection replay (`runner.rs`)

Build the full `Runner` struct:
- Holds connection config for both Postgres and target (host, port, user, database)
- `run(&self, ops: &[FrontendOp]) -> Result<(Vec<ResponseEvent>, Vec<ResponseEvent>)>`
  - Opens fresh connections to both
  - Runs template-driven setup SQL (deduplicated from enabled templates)
  - Encodes ops, respecting flush strategy (Sync/Query/Flush/CopyDone/CopyFail/Terminate → TCP flush, everything else → buffer)
  - Collects responses from both
  - Returns both response streams

Key detail: the same ops must be encoded and flushed identically to both connections. Don't
interleave — encode a batch to both, then flush both, then collect from both.

### 3.2 ResponseEvent normalization

Define a `ResponseEvent` enum that wraps `PgMessage` variants into comparable form:
- Extract fields we care about (e.g., `ErrorResponse` → code + message)
- Drop fields we ignore (e.g., file/line/routine)
- Include terminal events: `Timeout`, `Disconnected`
- Derive `PartialEq` + `Debug` for comparison and reporting

### 3.3 Comparator (`comparator.rs`)

Implement:
- `CompareRule` struct and `CompareAction` enum from design
- Default rules table
- `Comparator::compare(pg: &[ResponseEvent], target: &[ResponseEvent]) -> Option<Divergence>`
  - Filter both streams through rules (apply Skip, etc.)
  - Walk in lockstep, compare according to each message's action
  - Return first divergence found

Don't implement CLI rule overrides yet — hardcode the defaults. We'll add `--compare-rule`
parsing later.

### 3.4 End-to-end manual test

Update `main.rs` to:
1. Connect to both Postgres and a target
2. Run a few handcrafted sequences known to exercise non-obvious behavior
3. Compare and print divergences

Hardcoded sequences to try:
- Parse/Bind/Execute/Sync (happy path — should match)
- Parse → Query → Sync (simple query mid-extended-query)
- Parse("s1", ...) → Parse("s1", ...) → Sync (duplicate named statement)
- Execute with no prior Bind → Sync

This is the first time we can actually detect real divergences.

---

## Phase 4: Generation

### 4.1 Strategy trait and RandomStrategy (`generator.rs`)

Implement:
- `Strategy` trait from design
- `RandomStrategy`: uniform draws from enabled ops, geometric length distribution
- `Generator` struct: holds profile, filtered templates, strategy list, RNG seed
- `Generator::next(&mut self) -> Vec<FrontendOp>`

For op generation, needs helpers:
- Pick a random `SqlEntry` respecting affinity (simple vs extended context)
- Pick random `StmtName` / `PortalName` from the pools
- Generate random `Param` values matching a template's `param_count`

### 4.2 GrammarStrategy

Implement the state machine from the design:
- States: Idle, Extended, CopySimple, CopyExtended
- Weighted transitions with leak probability (~5%) for any-state ops
- Respects affinity tags when picking templates

### 4.3 First real fuzz run

Update `main.rs` to loop:
1. Generate a sequence
2. Run against both connections
3. Compare
4. Print any divergence with the full op sequence

Run for 100 iterations against a real Postgres (as both oracle and target — should find
zero divergences). Then point target at your database and see what surfaces.

---

## Phase 5: CLI and main loop

### 5.1 CLI argument parsing (`main.rs`)

Add argument parsing (use `clap`):
- `--postgres <url>` — oracle connection string
- `--target <url>` — target connection string
- `--seed <u64>` — RNG seed for reproducibility
- `--iterations <n>` — number of fuzz iterations (default: 1000)
- `--profile <preset>` — minimal / standard / full
- `--enable <tag>` / `--disable <tag>` — individual feature flags
- `--timeout <ms>` — per-collection response timeout (default: 1000)

Don't add `--compare-rule` or `--reuse-connections` yet.

### 5.2 Main loop with progress and summary

- Progress line: iteration count, divergences found so far, iterations/sec
- On divergence: print immediately, continue fuzzing
- Final summary: total iterations, unique divergence count, seed for reproduction
- Exit code: 0 if no divergences, 1 if any found

---

## Phase 6: Shrinking

### 6.1 ShrinkPass trait and basic passes (`shrinker.rs`)

Implement:
- `ShrinkPass` trait from design
- `SuffixTrim` — remove ops after the divergence point
- `SingleDeletion` — try removing each op one at a time
- Fixed-point loop: apply all passes until none makes progress

Each candidate re-runs through `Runner::run` + `Comparator::compare` with fresh
connections. "Same class of divergence" = same message kind mismatch at some point in the
response (doesn't need to be the exact same index, since deletion shifts things).

### 6.2 Advanced passes

- `PrefixTrim`
- `Subsequence` — binary search for shortest contiguous sub-sequence
- `NameSimplification` — try replacing named stmts/portals with unnamed
- `SqlSimplification` — try replacing each template with `SELECT 1`

### 6.3 Integrate shrinker into main loop

When a divergence is found:
1. Run shrinker
2. Report the minimized sequence (with a note of original length vs shrunk length)
3. Continue fuzzing

---

## Phase 7: Polish

### 7.1 Compare rule CLI overrides

Parse `--compare-rule 'ErrorResponse=Fields:[code]'` syntax. Apply overrides on top of
default rules table.

### 7.2 MutateStrategy

Save divergence-producing sequences to a corpus. `MutateStrategy` picks a corpus entry
and applies a random mutation (insert op, delete op, swap two ops, replace one op).

### 7.3 `--reuse-connections` mode

Between iterations, send `DISCARD ALL` + `Sync`, drain to `ReadyForQuery(Idle)`.
Reconnect if either server doesn't reach idle. Track whether the reset itself diverges.

### 7.4 Reporter trait and JSON output

- `Reporter` trait with `report_divergence()` and `report_summary()`
- Human-readable reporter (already have this from phase 5)
- JSON reporter (`--output-format json`) for CI integration
- JSON lines format: one object per divergence, summary object at end

### 7.5 Deduplication

Track divergence "signatures" (e.g., the message kind mismatch + the triggering op kind)
to avoid reporting the same class of bug repeatedly. Print count of duplicates suppressed
in summary.
