# pg_proto_fuzz

A protocol-level fuzzer for Postgres-compatible databases. Uses real Postgres as an oracle to verify that a target database produces identical wire-level behavior for arbitrary sequences of frontend messages.

## What it does

pg_proto_fuzz generates random sequences of Postgres wire protocol operations, sends the same sequence to both a real Postgres instance (the oracle) and a target database, then compares the response streams to surface behavioral divergences. This catches non-obvious differences like:

- Simple Query during an extended query batch causing implicit flush
- Error handling mid-pipeline (which messages are skipped until Sync?)
- Transaction state transitions reflected in ReadyForQuery
- Portal suspension and resumption semantics
- COPY sub-protocol entry/exit edge cases
- Behavior when referencing nonexistent statements/portals

When a divergence is found, the built-in shrinker minimizes the failing sequence to the smallest reproducing case.

## Architecture

```
Generator  →  Runner (dual-connection)  →  Comparator  →  Shrinker
   │                  │                        │              │
   │ Vec<FrontendOp>  │ (pg_resp, target_resp) │ Divergence?  │ minimal ops
   ▼                  ▼                        ▼              ▼
Produces structured   Replays ops against      Diffs the two  Minimizes the
protocol op           both servers with        response        failing sequence
sequences             fresh connections        streams
```

## Usage

```
cargo run -- \
  --pg-host localhost --pg-port 5432 --pg-user postgres --pg-database postgres \
  --target-host localhost --target-port 6432 --target-user postgres --target-database postgres \
  --iterations 1000 --seed 42 --profile standard --workers 10
```

### CLI flags

| Flag | Default | Description |
|------|---------|-------------|
| `--pg-host` | `localhost` | Oracle Postgres host |
| `--pg-port` | `5432` | Oracle Postgres port |
| `--pg-user` | `postgres` | Oracle Postgres user |
| `--pg-password` | — | Oracle Postgres password |
| `--pg-database` | `postgres` | Oracle Postgres database |
| `--target-host` | same as `--pg-host` | Target host |
| `--target-port` | same as `--pg-port` | Target port |
| `--target-user` | same as `--pg-user` | Target user |
| `--target-password` | same as `--pg-password` | Target password |
| `--target-database` | same as `--pg-database` | Target database |
| `-n`, `--iterations` | `1000` | Number of fuzz iterations |
| `--seed` | random | RNG seed for reproducibility |
| `--profile` | `minimal` | Feature profile: `minimal`, `standard`, `full` |
| `--enable` | — | Enable feature tags (comma-separated) |
| `--disable` | — | Disable feature tags (comma-separated) |
| `--timeout` | `2000` | Per-response-collection timeout in ms |
| `--workers` | `10` | Number of parallel workers |

### Feature profiles

| Profile | Tags enabled |
|---------|-------------|
| `minimal` | Core protocol ops only (Parse, Bind, Execute, Sync, etc.) |
| `standard` | `transactions`, `copy`, `multi_statement` |
| `full` | All known feature tags |

Individual tags can be toggled: `--profile standard --disable copy --enable plpgsql`.

### Feature tags

| Tag | What it gates |
|-----|---------------|
| `copy` | COPY sub-protocol via simple query |
| `copy_extended` | COPY via extended query |
| `transactions` | BEGIN / COMMIT / ROLLBACK |
| `sql_prepare` | SQL-level PREPARE / DEALLOCATE / EXECUTE |
| `plpgsql` | DO blocks, RAISE NOTICE |
| `function_call` | Protocol-level FunctionCall messages |
| `multi_statement` | Multi-statement strings in simple Query |

## Generation strategies

- **RandomStrategy** — Uniform random draws from the enabled operation vocabulary with geometric length distribution.
- **GrammarStrategy** — Weighted random walks through a state machine modeling expected protocol flow, with intentional "leak" probability for any-state ops.

## Shrinking

When a divergence is found, a multi-pass shrinker minimizes the sequence:

1. **SuffixTrim** — Remove ops after the divergence point
2. **SingleDeletion** — Try removing each individual op
3. **PrefixTrim** — Remove ops from the beginning
4. **NameSimplification** — Replace named statements/portals with unnamed
5. **SqlSimplification** — Replace SQL templates with simpler ones

Passes iterate to a fixed point.

## Example output

```
DIVERGENCE #1
  Sequence (3 ops):
    [0] Parse { name: "", sql: "SELECT 1", param_oids: [] }
    [1] Query { sql: "SELECT 2" }
    [2] Sync
  First mismatch at response #4 (after op #1):
    Postgres:  ReadyForQuery { status: Idle }
    Target:    DataRow { columns: ["1"] }
```

## Building

```
cargo build --release
```

Requires Rust 2024 edition.
