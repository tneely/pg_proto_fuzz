use rand::Rng;

use crate::op::{FrontendOp, Param, PortalName, StmtName};
use crate::profile::FuzzProfile;
use crate::template::{Affinity, SqlEntry, enabled_templates};

/// A generation strategy produces a single op sequence.
pub trait Strategy {
    fn generate(&mut self, templates: &[&SqlEntry], rng: &mut impl Rng) -> Vec<FrontendOp>;
}

/// The generator holds the profile, filtered templates, and strategies.
pub struct Generator {
    templates: Vec<&'static SqlEntry>,
    rng: rand::rngs::StdRng,
}

impl Generator {
    pub fn new(profile: &FuzzProfile, seed: u64) -> Self {
        use rand::SeedableRng;
        Self {
            templates: enabled_templates(profile),
            rng: rand::rngs::StdRng::seed_from_u64(seed),
        }
    }

    /// Generate the next op sequence, alternating between strategies.
    pub fn next(&mut self) -> Vec<FrontendOp> {
        let templates = self.templates.clone();
        // Alternate: 50% grammar-guided, 50% random
        if self.rng.random_bool(0.5) {
            let mut strategy = GrammarStrategy;
            strategy.generate(&templates, &mut self.rng)
        } else {
            let mut strategy = RandomStrategy;
            strategy.generate(&templates, &mut self.rng)
        }
    }
}

// ---------------------------------------------------------------------------
// RandomStrategy
// ---------------------------------------------------------------------------

/// Uniform random draws from the op vocabulary.
/// Sequence length follows a geometric distribution (mean ~8 ops).
pub struct RandomStrategy;

impl Strategy for RandomStrategy {
    fn generate(&mut self, templates: &[&SqlEntry], rng: &mut impl Rng) -> Vec<FrontendOp> {
        // Geometric distribution for length: each op has 12% chance of being the last
        let len = 1 + geometric(rng, 0.12, 30);
        let mut ops = Vec::with_capacity(len);
        for _ in 0..len {
            ops.push(random_op(templates, rng));
        }
        // Always end with Sync so we get a ReadyForQuery to collect
        if !ops
            .iter()
            .any(|op| matches!(op, FrontendOp::Sync | FrontendOp::Query { .. }))
        {
            ops.push(FrontendOp::Sync);
        }
        ops
    }
}

/// Generate a single random op.
fn random_op(templates: &[&SqlEntry], rng: &mut impl Rng) -> FrontendOp {
    // Weight categories: extended ops are more interesting than simple query
    let choice: u32 = rng.random_range(0..100);
    match choice {
        0..15 => random_parse(templates, rng),
        15..30 => random_bind(templates, rng),
        30..45 => random_execute(rng),
        45..55 => FrontendOp::Sync,
        55..65 => random_query(templates, rng),
        65..72 => FrontendOp::DescribeStatement {
            name: random_stmt_name(rng),
        },
        72..79 => FrontendOp::DescribePortal {
            name: random_portal_name(rng),
        },
        79..86 => FrontendOp::CloseStatement {
            name: random_stmt_name(rng),
        },
        86..93 => FrontendOp::ClosePortal {
            name: random_portal_name(rng),
        },
        93..97 => FrontendOp::Flush,
        97..99 => FrontendOp::CopyDone, // will usually be out of context — that's fine
        99..100 => FrontendOp::Terminate,
        _ => FrontendOp::Sync,
    }
}

// ---------------------------------------------------------------------------
// GrammarStrategy
// ---------------------------------------------------------------------------

/// State machine that loosely models the expected protocol flow.
/// Transitions have a "leak" probability that emits any op regardless of state.
pub struct GrammarStrategy;

#[derive(Debug, Clone, Copy, PartialEq)]
enum ProtoState {
    Idle,
    Extended,
    CopyIn,
}

impl Strategy for GrammarStrategy {
    fn generate(&mut self, templates: &[&SqlEntry], rng: &mut impl Rng) -> Vec<FrontendOp> {
        let len = 2 + geometric(rng, 0.10, 25);
        let mut ops = Vec::with_capacity(len);
        let mut state = ProtoState::Idle;
        let leak_prob = 0.05;

        for _ in 0..len {
            // Small chance of emitting any random op regardless of state
            if rng.random_bool(leak_prob) {
                ops.push(random_op(templates, rng));
                continue;
            }

            let op = match state {
                ProtoState::Idle => {
                    // Check if COPY-in templates are available
                    let has_copy_in = templates
                        .iter()
                        .any(|t| t.sql.contains("FROM STDIN") && t.affinity == Affinity::Simple);

                    if has_copy_in && rng.random_bool(0.1) {
                        // Start a COPY-in sequence via simple query
                        let tmpl = templates
                            .iter()
                            .find(|t| {
                                t.sql.contains("FROM STDIN") && t.affinity == Affinity::Simple
                            })
                            .unwrap();
                        state = ProtoState::CopyIn;
                        FrontendOp::Query {
                            sql: tmpl.sql.to_string(),
                        }
                    } else if rng.random_bool(0.3) {
                        // Simple query
                        let op = random_query(templates, rng);
                        // Simple query is self-contained, stays Idle
                        ops.push(op);
                        continue;
                    } else {
                        // Start extended query
                        state = ProtoState::Extended;
                        random_parse(templates, rng)
                    }
                }
                ProtoState::Extended => {
                    let choice: u32 = rng.random_range(0..100);
                    match choice {
                        0..25 => random_bind(templates, rng),
                        25..45 => random_execute(rng),
                        45..55 => FrontendOp::DescribeStatement {
                            name: random_stmt_name(rng),
                        },
                        55..65 => FrontendOp::DescribePortal {
                            name: random_portal_name(rng),
                        },
                        65..70 => FrontendOp::Flush,
                        70..80 => {
                            state = ProtoState::Idle;
                            FrontendOp::Sync
                        }
                        80..88 => FrontendOp::CloseStatement {
                            name: random_stmt_name(rng),
                        },
                        88..95 => FrontendOp::ClosePortal {
                            name: random_portal_name(rng),
                        },
                        95..100 => {
                            // Interleave a simple query mid-extended — deliberately interesting
                            random_query(templates, rng)
                        }
                        _ => FrontendOp::Sync,
                    }
                }
                ProtoState::CopyIn => {
                    let choice: u32 = rng.random_range(0..100);
                    match choice {
                        0..40 => FrontendOp::CopyData {
                            data: b"1\n".to_vec(),
                        },
                        40..70 => {
                            state = ProtoState::Idle;
                            FrontendOp::CopyDone
                        }
                        70..85 => {
                            state = ProtoState::Idle;
                            FrontendOp::CopyFail {
                                message: "abort".into(),
                            }
                        }
                        85..100 => {
                            // Leak: send a Sync during COPY — interesting edge case
                            state = ProtoState::Idle;
                            FrontendOp::Sync
                        }
                        _ => FrontendOp::CopyDone,
                    }
                }
            };
            ops.push(op);
        }

        // Ensure at least one Sync or Query so we get a ReadyForQuery
        if !ops
            .iter()
            .any(|op| matches!(op, FrontendOp::Sync | FrontendOp::Query { .. }))
        {
            ops.push(FrontendOp::Sync);
        }

        ops
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Geometric distribution: returns a value in [0, max).
fn geometric(rng: &mut impl Rng, p: f64, max: usize) -> usize {
    let mut n = 0;
    while n < max - 1 && !rng.random_bool(p) {
        n += 1;
    }
    n
}

fn random_stmt_name(rng: &mut impl Rng) -> StmtName {
    StmtName::POOL[rng.random_range(0..StmtName::POOL.len())].clone()
}

fn random_portal_name(rng: &mut impl Rng) -> PortalName {
    PortalName::POOL[rng.random_range(0..PortalName::POOL.len())].clone()
}

fn random_template<'a>(
    templates: &[&'a SqlEntry],
    affinity_filter: Option<Affinity>,
    rng: &mut impl Rng,
) -> &'a SqlEntry {
    let candidates: Vec<&&SqlEntry> = templates
        .iter()
        .filter(|t| match affinity_filter {
            None => true,
            Some(Affinity::Any) => true,
            Some(a) => t.affinity == a || t.affinity == Affinity::Any,
        })
        .collect();
    if candidates.is_empty() {
        // Fallback to any template
        templates[rng.random_range(0..templates.len())]
    } else {
        candidates[rng.random_range(0..candidates.len())]
    }
}

fn random_params(count: usize, rng: &mut impl Rng) -> Vec<Param> {
    (0..count)
        .map(|_| {
            let choice: u32 = rng.random_range(0..100);
            match choice {
                0..10 => Param::Null,
                10..50 => Param::Int32(rng.random_range(-100..100)),
                50..90 => Param::Text(format!("v{}", rng.random_range(0..100))),
                _ => Param::Bytes(vec![rng.random::<u8>(); rng.random_range(0..8)]),
            }
        })
        .collect()
}

fn random_parse(templates: &[&SqlEntry], rng: &mut impl Rng) -> FrontendOp {
    let tmpl = random_template(templates, Some(Affinity::Extended), rng);
    FrontendOp::Parse {
        name: random_stmt_name(rng),
        sql: tmpl.sql.to_string(),
        param_oids: vec![], // let the server infer types
    }
}

fn random_bind(templates: &[&SqlEntry], rng: &mut impl Rng) -> FrontendOp {
    // Pick a template to know how many params to generate
    let tmpl = random_template(templates, Some(Affinity::Extended), rng);
    FrontendOp::Bind {
        portal: random_portal_name(rng),
        stmt: random_stmt_name(rng),
        params: random_params(tmpl.param_count, rng),
    }
}

fn random_execute(rng: &mut impl Rng) -> FrontendOp {
    FrontendOp::Execute {
        portal: random_portal_name(rng),
        max_rows: if rng.random_bool(0.8) {
            0
        } else {
            rng.random_range(1..10)
        },
    }
}

fn random_query(templates: &[&SqlEntry], rng: &mut impl Rng) -> FrontendOp {
    let tmpl = random_template(templates, Some(Affinity::Simple), rng);
    FrontendOp::Query {
        sql: tmpl.sql.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use std::collections::HashSet;

    #[test]
    fn test_random_strategy_produces_ops() {
        let profile = FuzzProfile::minimal();
        let templates = enabled_templates(&profile);
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut strategy = RandomStrategy;

        let ops = strategy.generate(&templates, &mut rng);
        assert!(!ops.is_empty());
        // Must end with at least one Sync or Query
        assert!(
            ops.iter()
                .any(|op| matches!(op, FrontendOp::Sync | FrontendOp::Query { .. }))
        );
    }

    #[test]
    fn test_grammar_strategy_produces_ops() {
        let profile = FuzzProfile::minimal();
        let templates = enabled_templates(&profile);
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut strategy = GrammarStrategy;

        let ops = strategy.generate(&templates, &mut rng);
        assert!(!ops.is_empty());
        assert!(
            ops.iter()
                .any(|op| matches!(op, FrontendOp::Sync | FrontendOp::Query { .. }))
        );
    }

    #[test]
    fn test_generator_deterministic_with_seed() {
        let profile = FuzzProfile::minimal();
        let mut gen1 = Generator::new(&profile, 123);
        let mut gen2 = Generator::new(&profile, 123);

        for _ in 0..10 {
            let ops1 = gen1.next();
            let ops2 = gen2.next();
            assert_eq!(ops1, ops2, "same seed should produce identical sequences");
        }
    }

    #[test]
    fn test_generator_different_seeds_differ() {
        let profile = FuzzProfile::minimal();
        let mut gen1 = Generator::new(&profile, 1);
        let mut gen2 = Generator::new(&profile, 2);

        // Collect several sequences and check they're not all identical
        let seqs1: Vec<_> = (0..10).map(|_| gen1.next()).collect();
        let seqs2: Vec<_> = (0..10).map(|_| gen2.next()).collect();
        assert_ne!(seqs1, seqs2);
    }

    #[test]
    fn test_profile_filters_templates() {
        let minimal = FuzzProfile::minimal();
        let full = FuzzProfile::full();

        let min_templates = enabled_templates(&minimal);
        let full_templates = enabled_templates(&full);

        assert!(
            full_templates.len() > min_templates.len(),
            "full profile should have more templates than minimal"
        );

        // Minimal should not include COPY or transaction templates
        for t in &min_templates {
            assert!(
                t.requires.is_empty(),
                "minimal profile template {:?} has requirements",
                t.sql
            );
        }
    }

    #[test]
    fn test_variety_of_ops_generated() {
        let profile = FuzzProfile::full();
        let mut generator = Generator::new(&profile, 42);

        let mut op_kinds = HashSet::new();
        for _ in 0..100 {
            for op in generator.next() {
                let kind = match &op {
                    FrontendOp::Query { .. } => "Query",
                    FrontendOp::Parse { .. } => "Parse",
                    FrontendOp::Bind { .. } => "Bind",
                    FrontendOp::Execute { .. } => "Execute",
                    FrontendOp::Sync => "Sync",
                    FrontendOp::Flush => "Flush",
                    FrontendOp::DescribeStatement { .. } => "DescribeStatement",
                    FrontendOp::DescribePortal { .. } => "DescribePortal",
                    FrontendOp::CloseStatement { .. } => "CloseStatement",
                    FrontendOp::ClosePortal { .. } => "ClosePortal",
                    FrontendOp::CopyData { .. } => "CopyData",
                    FrontendOp::CopyDone => "CopyDone",
                    FrontendOp::CopyFail { .. } => "CopyFail",
                    FrontendOp::Terminate => "Terminate",
                };
                op_kinds.insert(kind);
            }
        }

        // Should generate at least the core extended query ops
        assert!(op_kinds.contains("Parse"));
        assert!(op_kinds.contains("Bind"));
        assert!(op_kinds.contains("Execute"));
        assert!(op_kinds.contains("Sync"));
        assert!(op_kinds.contains("Query"));
    }
}
