# pg_proto_fuzz — Design

A protocol-level fuzzer for Postgres-compatible databases. Uses real Postgres as an oracle
to verify that a target database produces identical wire-level behavior for arbitrary
sequences of frontend messages.

## Goal

Surface every behavioral divergence between a target database and real Postgres at the
wire protocol layer. This includes non-obvious implicit behaviors such as:

- Simple Query during an extended query batch causing implicit Flush and closure of the
  unnamed prepared statement
- Error handling mid-pipeline (which messages are skipped until Sync?)
- Transaction state transitions reflected in ReadyForQuery
- Portal suspension and resumption semantics
- COPY sub-protocol entry/exit edge cases
- Behavior when referencing nonexistent statements/portals
- Overlapping named/unnamed statement and portal lifecycles

## Architecture

```
┌───────────────┐
│   Generator   │  Produces sequences of frontend messages
│  (msg seqs)   │  from a grammar of protocol operations
└──────┬────────┘
       │  Vec<FrontendOp>
       ▼
┌───────────────┐
│    Runner     │  Sends the same sequence to both Postgres
│  (dual conn)  │  and the target, collects response streams
└──────┬────────┘
       │  (Vec<PgMessage>, Vec<PgMessage>)
       ▼
┌───────────────┐
│  Comparator   │  Diffs the two response streams, produces
│               │  a structured divergence report
└──────┬────────┘
       │  Option<Divergence>
       ▼
┌───────────────┐
│   Shrinker    │  Minimizes the failing sequence to the
│               │  smallest reproducing case
└───────────────┘
```

## Feature tags

The target database may not support every Postgres feature. Rather than a fixed struct of
booleans, features are identified by string tags. This means adding a new feature is purely
additive — define the tag, label the relevant templates and ops, done.

```rust
/// A feature tag is just a string. No central enum to update.
type Feature = &'static str;

struct FuzzProfile {
    /// The set of enabled feature tags. Templates and ops whose required
    /// tags are all present in this set are included in the generator's
    /// vocabulary. Templates with no required tags are always included.
    enabled: HashSet<Feature>,
}
```

**Built-in tags (initially):**

| Tag | What it gates |
|-----|---------------|
| `copy` | COPY sub-protocol via simple query |
| `copy_extended` | COPY via extended query (Parse/Bind/Execute a COPY). Implies `copy`. |
| `transactions` | BEGIN / COMMIT / ROLLBACK |
| `sql_prepare` | SQL-level PREPARE / DEALLOCATE / EXECUTE |
| `plpgsql` | DO blocks, RAISE NOTICE |
| `function_call` | Protocol-level FunctionCall messages |
| `multi_statement` | Multi-statement strings in simple Query |

Core protocol ops (Query, Parse, Bind, Describe, Execute, Close, Sync, Flush) are
ungated — they're always in the vocabulary.

**Adding a new feature** (e.g., `listen_notify`, `cursors`, `savepoints`) requires:

1. Pick a tag name.
2. Add templates to the template registry tagged with it.
3. Optionally add new `FrontendOp` variants if the feature introduces new wire messages.
4. Optionally add setup SQL to the runner's setup phase.

No changes to `FuzzProfile`, the generator, the comparator, or the CLI parsing code.

**Presets** are named sets of tags for convenience:

| Preset | Tags enabled |
|--------|--------------|
| `--profile minimal` | (none — core ops only) |
| `--profile standard` | `transactions`, `copy`, `multi_statement` |
| `--profile full` | all known tags |

Individual flags add or remove tags: `--profile standard --no-copy --enable plpgsql`.
`--enable <tag>` works for any string, including tags you define yourself — the system
doesn't need to know about them ahead of time.

## State isolation

Named prepared statements and portals persist for the lifetime of a connection. Without
care, this creates two problems:

1. **Cross-iteration pollution.** If iteration N creates statement `"s1"` and doesn't close
   it, iteration N+1's `Parse { name: "s1", ... }` will fail with "already exists". Over
   time the fuzz space collapses into error-handling paths only.

2. **Non-reproducible divergences.** A divergence in iteration N+1 might depend on leftover
   state from iteration N, making the reported sequence insufficient to reproduce the bug.

**Solution: fresh connections per iteration.** Each fuzz iteration establishes a new
connection pair (one to Postgres, one to the target). This guarantees:

- Every sequence starts from identical, clean state
- Divergences are self-contained and reproducible
- No accumulation of leaked statements/portals

Connection overhead is acceptable — the fuzzer is I/O-bound on response collection, not
on connection setup. For targets where connection setup is expensive, we provide an
alternative mode:

**Reset mode (opt-in via `--reuse-connections`).** Reuse connections across iterations by
issuing a reset sequence between them:

```
Query { "DISCARD ALL" }   -- resets all session state
Sync
-- drain responses, assert ReadyForQuery(Idle) from both
```

If either server doesn't return to `ReadyForQuery(Idle)` after reset, tear down and
reconnect. This mode is faster but has a subtle implication: `DISCARD ALL` itself is a
statement whose behavior we're comparing, so if the target doesn't implement it faithfully,
reset-mode may mask or introduce spurious divergences. Fresh connections avoid this
entirely.

**Within-iteration dirty state is intentional.** The generator *should* produce sequences
that create state conflicts within a single iteration — re-parsing over an existing named
statement, binding to a closed portal, etc. These are exactly the edge cases we want to
test. Isolation is only between iterations, never within them.

## Components

### 1. FrontendOp — The operation vocabulary

Rather than fuzzing raw bytes, we fuzz *structured* protocol operations. Each `FrontendOp`
maps to one or more frontend wire messages. This keeps generated sequences syntactically
valid at the frame level while allowing semantically arbitrary (and often illegal) sequences.

```rust
enum FrontendOp {
    // Simple query protocol
    Query { sql: SqlTemplate },

    // Extended query protocol
    Parse { name: StmtName, sql: SqlTemplate, param_oids: Vec<Oid> },
    Bind { portal: PortalName, stmt: StmtName, params: Vec<Param> },
    DescribeStatement { name: StmtName },
    DescribePortal { name: PortalName },
    Execute { portal: PortalName, max_rows: i32 },
    CloseStatement { name: StmtName },
    ClosePortal { name: PortalName },
    Sync,
    Flush,

    // COPY sub-protocol
    CopyData { data: Bytes },
    CopyDone,
    CopyFail { message: String },

    // Other
    FunctionCall { oid: Oid, args: Vec<Bytes> },
    Terminate,
}
```

**Name pools.** Statement and portal names are drawn from a small fixed pool
(`""`, `"s1"`, `"s2"` for statements; `""`, `"p1"`, `"p2"` for portals). This keeps the
search space small while still exercising named/unnamed interactions and name collisions.

**SQL template registry.** Templates are declarative data, not code. Each entry is a struct:

```rust
struct SqlEntry {
    sql: &'static str,
    /// Which protocol path this is designed for. Advisory — the generator
    /// may deliberately violate it at low probability.
    affinity: Affinity,          // Simple, Extended, Any
    /// Feature tags required. All must be present in the profile.
    requires: &'static [Feature],
    /// Number of parameters (for Bind). 0 for non-parameterized.
    param_count: usize,
    /// Setup SQL to run once at connection start (e.g., CREATE TABLE).
    /// Empty if none needed.
    setup: &'static [&'static str],
}
```

Adding a new template = appending an entry. No other code changes. The generator draws
from the subset of entries whose `requires` tags are all enabled.

**Initial registry:**

| SQL | Affinity | Requires | Params | Purpose |
|-----|----------|----------|--------|---------|
| `SELECT 1` | Any | — | 0 | Trivial success |
| `SELECT $1::int` | Extended | — | 1 | Parameterized query |
| `SELECT $1::int, $2::text` | Extended | — | 2 | Multi-param binding |
| `SELECT * FROM pg_type LIMIT 5` | Any | — | 0 | Real rows, RowDescription variety |
| `SLECT 1` (typo) | Any | — | 0 | Parse error |
| `SELECT 1/0` | Any | — | 0 | Runtime error |
| `SELECT 1; SELECT 2` | Simple | `multi_statement` | 0 | Multi-statement |
| `BEGIN` | Any | `transactions` | 0 | Enter transaction |
| `COMMIT` | Any | `transactions` | 0 | Commit |
| `ROLLBACK` | Any | `transactions` | 0 | Abort |
| `COPY (SELECT 1) TO STDOUT` | Simple | `copy` | 0 | COPY-out, simple path |
| `COPY (SELECT 1) TO STDOUT` | Extended | `copy_extended` | 0 | COPY-out, extended path |
| `COPY copy_test FROM STDIN` | Simple | `copy` | 0 | COPY-in, simple path |
| `COPY copy_test FROM STDIN` | Extended | `copy_extended` | 0 | COPY-in, extended path |
| `DO $$ BEGIN RAISE NOTICE 'hi'; END $$` | Any | `plpgsql` | 0 | Async notice |
| `PREPARE fuzz_stmt AS SELECT 1` | Simple | `sql_prepare` | 0 | SQL-level prepare |
| `DEALLOCATE fuzz_stmt` | Simple | `sql_prepare` | 0 | SQL-level deallocate |
| `EXECUTE fuzz_stmt` | Simple | `sql_prepare` | 0 | SQL-level execute |
| `DEALLOCATE ALL` | Simple | `sql_prepare` | 0 | Nuke all prepared stmts |

The same SQL (`COPY ... TO STDOUT`) appears as separate entries for simple vs extended
paths because the behavior differs substantially. In simple query, COPY-out streams
`CopyOutResponse → CopyData* → CopyDone → CommandComplete → ReadyForQuery`. In extended
query, the server enters COPY mode after `Execute` and the interaction with subsequent
pipelined messages has its own edge cases — e.g., what happens if you pipeline an Execute
of a COPY-in followed by CopyData followed by another Execute?

The `Affinity` tag is advisory to the generator — it won't emit a Simple-only template
inside a Parse, and won't emit an Extended-only template inside a Query — but it's free to
ignore the advice at low probability to test what happens when you do the "wrong" thing.

The `setup` field lets templates declare their own prerequisites. For example, COPY-in
templates set `setup: &["CREATE TABLE IF NOT EXISTS copy_test (id int)"]`. The runner
collects all setup SQL from enabled templates and runs it once per connection. This means
new templates that need scratch tables are self-contained — no separate runner changes.

### 2. Generator

The generator produces `Vec<FrontendOp>` sequences. It holds a reference to the active
`FuzzProfile` and a filtered snapshot of the template registry — only entries whose tags
are all enabled. Adding new templates or feature tags doesn't require generator changes.

Generation is delegated to **strategies** behind a common trait:

```rust
trait Strategy {
    fn generate(&mut self, templates: &[SqlEntry], rng: &mut impl Rng) -> Vec<FrontendOp>;
}
```

Each strategy is a self-contained generation algorithm. The generator picks a strategy per
iteration (weighted random, or round-robin, or caller-specified). Adding a new strategy =
implementing the trait and registering it. Existing strategies are unaffected.

**Built-in strategies:**

**RandomStrategy.** Uniform random draws from the enabled operation vocabulary, biased
toward shorter sequences (geometric distribution, mean ~10 ops). This catches completely
unexpected interactions.

**GrammarStrategy.** Weighted random walks through a state machine that loosely models the
expected protocol flow. Transitions are intentionally "leaky" — there's always a small
probability of emitting any operation regardless of state. This concentrates effort on
semi-valid sequences that are more likely to reach interesting server states.

```
         ┌─────────┐
    ┌───▶│  Idle   │◀──── Sync
    │    └────┬────┘
    │         │ Parse / Query
    │    ┌────▼─────┐
    │    │ Extended  │──── Bind, Describe, Execute, Flush
    │    └──┬────┬──┘
    │       │    │ Execute of COPY-in SQL (extended path)
    │       │    ▼
    │       │  ┌──────────────┐
    │       │  │ COPY-Extended │──── CopyData, CopyDone, CopyFail
    │       │  └──────┬───────┘     then: more Executes? Sync?
    │       │         │              what about pipelined ops?
    │       │         ▼
    │       │    back to Extended (pending pipeline) or Idle (after Sync)
    │       │
    │       │ Query with COPY SQL (simple path)
    │    ┌──▼──────┐
    │    │COPY-Simp│──── CopyData, CopyDone, CopyFail
    │    └────┬────┘
    │         │
    └─────────┘
```

The two COPY paths are modeled separately because their edge cases differ:

- **Simple-query COPY-in**: Server sends `CopyInResponse`, client sends `CopyData*` then
  `CopyDone`/`CopyFail`, server sends `CommandComplete` + `ReadyForQuery`. Straightforward.

- **Extended-query COPY-in**: Server sends `CopyInResponse` after `Execute`. But the
  client may have pipelined more messages after that Execute (another Bind/Execute, a Sync,
  a Query...). What happens to those? Does CopyDone terminate COPY mode and let the
  pipeline resume? What if Sync arrives during COPY-in? These are the exact edge cases we
  want to fuzz. The generator models this by allowing any op to follow an Execute-of-COPY,
  not just CopyData.

**MutateStrategy.** When a divergence is found, the generator saves the sequence. This
strategy replays mutations of saved sequences (insert, delete, swap, replace single ops).
It's automatically activated once the corpus is non-empty.

### 3. Runner

The runner manages two TCP connections — one to real Postgres, one to the target — and
replays the same `Vec<FrontendOp>` against both.

**Connection lifecycle (per iteration, unless `--reuse-connections`):**

1. Connect to both servers using `pg_stream::ConnectionBuilder` with identical params
2. Collect and run setup SQL from all enabled templates (deduplicated). Templates declare
   their own setup needs via the `setup` field — no central list to maintain.
3. Encode each `FrontendOp` into the connection's write buffer via `PgProtocol` methods
4. Flush after each logical batch (or per-op if the op is Flush/Sync/Query)
5. Collect responses until `ReadyForQuery` or connection close
6. Return two `Vec<PgMessage>` response streams
7. Disconnect (or reset if reusing connections)

**Flush strategy.** This is subtle. In the real protocol, messages are buffered and only
sent when the TCP buffer flushes. We need to be precise about *when* we flush the TCP
stream to match real client behavior:

- `Sync` → flush immediately (Sync always triggers a server response cycle)
- `Query` → flush immediately (simple query is self-contained)
- `Flush` → flush immediately (explicit flush request)
- `CopyDone` / `CopyFail` → flush immediately
- `Terminate` → flush immediately
- All other ops → buffer (they're part of an extended query batch)

**Timeouts.** Each response collection phase has a timeout. If one server responds and the
other doesn't within the timeout, that's a divergence (the target is hanging or has
mismatched flush semantics).

**Error recovery.** If a connection enters an error state, we attempt to recover with
Sync. If the connection is broken, we record the point of failure and report it.

### 4. Comparator

Compares two response streams message-by-message. Not a byte-level comparison — we need
semantic comparison that ignores fields which legitimately differ between servers.

Comparison behavior is driven by a table of **compare rules** — declarative data, not
hardcoded match arms:

```rust
struct CompareRule {
    /// Which message type this rule applies to.
    message: MessageKind,
    /// What to do with this message type.
    action: CompareAction,
}

enum CompareAction {
    /// Skip entirely (don't include in comparison stream).
    Skip,
    /// Compare presence/type only — the message must appear in the same
    /// position but its contents are not inspected.
    PresenceOnly,
    /// Compare specific fields by name. Fields not listed are ignored.
    Fields(Vec<&'static str>),
}
```

**Default rules:**

| Message | Action |
|---------|--------|
| ReadyForQuery | Fields: `[status]` |
| ErrorResponse | Fields: `[code, message]` |
| NoticeResponse | Fields: `[code, message]` |
| RowDescription | Fields: `[column_count, names, type_oids, format]` |
| DataRow | Fields: `[all]` |
| CommandComplete | Fields: `[tag]` |
| ParameterDescription | Fields: `[param_count, oids]` |
| CopyInResponse / CopyOutResponse | Fields: `[format, column_formats]` |
| ParseComplete, BindComplete, CloseComplete, NoData, EmptyQueryResponse | PresenceOnly |
| PortalSuspended | PresenceOnly |
| BackendKeyData | Skip |
| ParameterStatus | Skip |
| NotificationResponse | Fields: `[channel, payload]` |

Rules are configurable at runtime. A target that doesn't match Postgres error messages
exactly can relax ErrorResponse to `Fields: [code]` only. A target that doesn't yet emit
NoticeResponse can add a Skip rule for it. This is done via CLI or a config file, not code
changes:

```
--compare-rule 'ErrorResponse=Fields:[code]'
--compare-rule 'NoticeResponse=Skip'
```

Adding comparison support for a new message type = adding a default rule entry and the
field extraction logic for that message kind.

**Comparison output:** A `Divergence` struct recording the operation index, the expected
(Postgres) message, and the actual (target) message.

```rust
struct Divergence {
    /// Index into the FrontendOp sequence where behavior diverged
    op_index: usize,
    /// Index into the response stream where the first mismatch occurred
    response_index: usize,
    /// The expected response (from Postgres)
    expected: ResponseEvent,
    /// The actual response (from target)
    actual: ResponseEvent,
    /// Full operation sequence for reproduction
    ops: Vec<FrontendOp>,
}
```

### 5. Shrinker

When a divergence is found, the shrinker minimizes the operation sequence while preserving
the divergence. This is critical for producing actionable bug reports.

Shrinking is organized as a pipeline of **passes**, each behind a common trait:

```rust
trait ShrinkPass {
    /// Given a sequence that produces a divergence, return a shorter
    /// sequence that produces the same class of divergence, or None
    /// if this pass can't shrink further.
    fn shrink(
        &self,
        ops: &[FrontendOp],
        divergence: &Divergence,
        runner: &Runner,
        comparator: &Comparator,
    ) -> Option<Vec<FrontendOp>>;
}
```

Passes are applied in order, iterating until no pass makes progress (fixed point). Adding
a new shrink strategy = implementing the trait and appending it to the pass list.

**Built-in passes:**

1. **PrefixTrim.** Try removing ops from the beginning of the sequence.
2. **SuffixTrim.** Try removing ops after the divergence point.
3. **SingleDeletion.** Try removing each individual op.
4. **Subsequence.** Try contiguous sub-sequences.
5. **NameSimplification.** Replace named statements/portals with unnamed.
6. **SqlSimplification.** Replace SQL templates with simpler ones (`SELECT 1`).

Each candidate is re-run through Runner + Comparator. If the same class of divergence
persists, the shorter sequence replaces the current one.

## Execution flow

```
main:
    parse CLI args (postgres url, target url, seed, iterations, profile)
    build FuzzProfile from profile preset + flag overrides
    build Generator with profile
    verify connectivity to both servers (one-shot connect + disconnect)

    loop for N iterations:
        seq = generator.next()
        -- fresh connections per iteration (or reset if --reuse-connections)
        (pg_resp, target_resp) = runner.run(seq)
        if let Some(div) = comparator.compare(pg_resp, target_resp):
            minimal = shrinker.shrink(seq)  -- shrinker uses fresh connections too
            report(minimal)
    print summary (total iterations, divergences found, unique divergence classes)
```

## Output format

Each divergence is reported as:

```
DIVERGENCE #1
  Sequence (3 ops):
    [0] Parse { name: "", sql: "SELECT 1", param_oids: [] }
    [1] Query { sql: "SELECT 2" }
    [2] Sync
  First mismatch at response #4 (after op #1):
    Postgres:  ReadyForQuery { status: Idle }
    Target:    DataRow { columns: ["1"] }
  Note: Simple Query during extended query batch — Postgres implicitly
        flushes pending extended query messages.
```

## Non-goals (for now)

- **SQL fuzzing.** We fuzz protocol sequences, not SQL syntax. SQL is drawn from a fixed
  template pool. SQL fuzzing is a separate, complementary effort.
- **TLS/auth fuzzing.** We assume both connections complete startup identically. The
  fuzzer begins after the startup handshake.
- **Performance comparison.** We only compare correctness, not timing.
- **Multi-connection scenarios.** Single connection per run. LISTEN/NOTIFY cross-connection
  behavior is out of scope.

## Extension points — summary

The system is designed so that the most common extensions are purely additive (no existing
code changes) and the less common ones are localized.

| I want to... | What to do | Files touched |
|--------------|------------|---------------|
| Add a SQL template | Append an `SqlEntry` to the registry | `template.rs` only |
| Add a feature tag | Pick a name, tag templates with it | `template.rs` only |
| Add a feature tag with new wire messages | Add `FrontendOp` variant, tag templates, handle in runner encode + comparator | `op.rs`, `template.rs`, `runner.rs`, `comparator.rs` |
| Add a generation strategy | Implement `Strategy` trait, register it | new file or `generator.rs` |
| Add a shrink strategy | Implement `ShrinkPass` trait, register it | `shrinker.rs` |
| Change what fields are compared for a message | Edit the default compare rules table, or use `--compare-rule` at runtime | `comparator.rs` or CLI |
| Add setup SQL for a new template | Set the `setup` field on the `SqlEntry` | `template.rs` only |
| Add a new output format | Implement `Reporter` trait | new file or `report.rs` |
| Support a new connection mode (e.g., TLS, unix socket) | Extend `ConnectionConfig` | `runner.rs` |

The guiding principle: **data changes (templates, tags, rules) should never require logic
changes.** Logic changes (new ops, new strategies) should be localized to one or two files
and never require updating unrelated components.

`FrontendOp` is intentionally a plain enum, not a trait object. Adding a variant gives you
compiler errors at every match arm that needs updating — which is exactly the safety net
you want when a new wire message touches the runner, comparator, shrinker, and report
formatter. The enum is the one place where exhaustive handling is more valuable than
open-ended extensibility.

## Crate structure

```
src/
  main.rs          — CLI entry point, argument parsing, main loop
  profile.rs       — FuzzProfile (tag set), presets, CLI flag merging
  op.rs            — FrontendOp enum, StmtName, PortalName, Param
  template.rs      — SqlEntry registry, feature tags, protocol affinity, setup SQL
  generator.rs     — Strategy trait, built-in strategies, profile-filtered draws
  runner.rs        — Dual-connection replay, template-driven setup, response collection
  comparator.rs    — CompareRule table, semantic response comparison, CLI overrides
  shrinker.rs      — ShrinkPass trait, built-in passes, fixed-point loop
  report.rs        — Reporter trait, human-readable + JSON formatters
```
