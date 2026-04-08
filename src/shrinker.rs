use crate::comparator::{self, Divergence, NormalizedMsg};
use crate::connection::ConnectionFactory;
use crate::op::{FrontendOp, PortalName, StmtName};
use crate::runner::DualRunner;

/// Classification of a divergence — used to check if a shrunk sequence
/// reproduces the "same" bug. Two divergences are in the same class if
/// they have the same message-kind mismatch (expected vs actual).
#[derive(Debug, Clone, PartialEq)]
struct DivergenceClass {
    expected_kind: String,
    actual_kind: String,
}

fn classify(div: &Divergence) -> DivergenceClass {
    DivergenceClass {
        expected_kind: msg_kind(&div.expected),
        actual_kind: msg_kind(&div.actual),
    }
}

fn msg_kind(msg: &Option<NormalizedMsg>) -> String {
    match msg {
        None => "EndOfStream".into(),
        Some(m) => match m {
            NormalizedMsg::ReadyForQuery { .. } => "ReadyForQuery",
            NormalizedMsg::ErrorResponse { .. } => "ErrorResponse",
            NormalizedMsg::NoticeResponse { .. } => "NoticeResponse",
            NormalizedMsg::RowDescription { .. } => "RowDescription",
            NormalizedMsg::DataRow { .. } => "DataRow",
            NormalizedMsg::CommandComplete { .. } => "CommandComplete",
            NormalizedMsg::ParameterDescription { .. } => "ParameterDescription",
            NormalizedMsg::CopyInResponse { .. } => "CopyInResponse",
            NormalizedMsg::CopyOutResponse { .. } => "CopyOutResponse",
            NormalizedMsg::CopyData { .. } => "CopyData",
            NormalizedMsg::ParseComplete => "ParseComplete",
            NormalizedMsg::BindComplete => "BindComplete",
            NormalizedMsg::CloseComplete => "CloseComplete",
            NormalizedMsg::NoData => "NoData",
            NormalizedMsg::EmptyQueryResponse => "EmptyQueryResponse",
            NormalizedMsg::PortalSuspended => "PortalSuspended",
            NormalizedMsg::CopyDone => "CopyDone",
            NormalizedMsg::Timeout => "Timeout",
            NormalizedMsg::Disconnected(_) => "Disconnected",
        }
        .into(),
    }
}

/// Test whether a candidate op sequence reproduces the same class of divergence.
async fn reproduces<F1: ConnectionFactory, F2: ConnectionFactory>(
    candidate: &[FrontendOp],
    class: &DivergenceClass,
    runner: &DualRunner<F1, F2>,
) -> bool {
    if candidate.is_empty() {
        return false;
    }
    match runner.run(candidate).await {
        Ok((pg, target)) => {
            if let Some(div) = comparator::compare(&pg, &target, candidate) {
                classify(&div) == *class
            } else {
                false
            }
        }
        Err(_) => false,
    }
}

// ---------------------------------------------------------------------------
// Passes
// ---------------------------------------------------------------------------

/// Remove ops from the end. Tries the most aggressive trim first.
async fn suffix_trim<F1: ConnectionFactory, F2: ConnectionFactory>(
    ops: &[FrontendOp],
    class: &DivergenceClass,
    runner: &DualRunner<F1, F2>,
) -> Option<Vec<FrontendOp>> {
    for end in 1..ops.len() {
        let candidate = ops[..end].to_vec();
        if reproduces(&candidate, class, runner).await {
            return Some(candidate);
        }
    }
    None
}

/// Remove ops from the beginning. Tries the most aggressive trim first.
async fn prefix_trim<F1: ConnectionFactory, F2: ConnectionFactory>(
    ops: &[FrontendOp],
    class: &DivergenceClass,
    runner: &DualRunner<F1, F2>,
) -> Option<Vec<FrontendOp>> {
    for start in (1..ops.len()).rev() {
        let candidate = ops[start..].to_vec();
        if reproduces(&candidate, class, runner).await {
            return Some(candidate);
        }
    }
    None
}

/// Try removing each individual op, one at a time.
async fn single_deletion<F1: ConnectionFactory, F2: ConnectionFactory>(
    ops: &[FrontendOp],
    class: &DivergenceClass,
    runner: &DualRunner<F1, F2>,
) -> Option<Vec<FrontendOp>> {
    for i in 0..ops.len() {
        let mut candidate = ops.to_vec();
        candidate.remove(i);
        if reproduces(&candidate, class, runner).await {
            return Some(candidate);
        }
    }
    None
}

/// Binary search for the shortest contiguous sub-sequence that reproduces.
async fn subsequence<F1: ConnectionFactory, F2: ConnectionFactory>(
    ops: &[FrontendOp],
    class: &DivergenceClass,
    runner: &DualRunner<F1, F2>,
) -> Option<Vec<FrontendOp>> {
    let n = ops.len();
    if n <= 2 {
        return None;
    }

    let mut best: Option<Vec<FrontendOp>> = None;
    let mut lo: usize = 1;
    let mut hi: usize = n - 1;

    while lo <= hi {
        let mid = (lo + hi) / 2;
        let mut found = false;

        for start in 0..=(n - mid) {
            let candidate = ops[start..start + mid].to_vec();
            if reproduces(&candidate, class, runner).await {
                best = Some(candidate);
                found = true;
                break;
            }
        }

        if found {
            if mid <= 1 {
                break;
            }
            hi = mid - 1;
        } else {
            lo = mid + 1;
        }
    }

    best.filter(|b| b.len() < n)
}

/// Try replacing named statements/portals with unnamed.
async fn name_simplification<F1: ConnectionFactory, F2: ConnectionFactory>(
    ops: &[FrontendOp],
    class: &DivergenceClass,
    runner: &DualRunner<F1, F2>,
) -> Option<Vec<FrontendOp>> {
    let mut current = ops.to_vec();
    let mut progress = false;

    for i in 0..current.len() {
        if let Some(simplified) = simplify_names(&current[i]) {
            let mut candidate = current.clone();
            candidate[i] = simplified;
            if reproduces(&candidate, class, runner).await {
                current = candidate;
                progress = true;
            }
        }
    }

    if progress { Some(current) } else { None }
}

fn simplify_names(op: &FrontendOp) -> Option<FrontendOp> {
    match op {
        FrontendOp::Parse {
            name,
            sql,
            param_oids,
        } if *name != StmtName::Unnamed => Some(FrontendOp::Parse {
            name: StmtName::Unnamed,
            sql: sql.clone(),
            param_oids: param_oids.clone(),
        }),
        FrontendOp::Bind {
            portal,
            stmt,
            params,
        } if *portal != PortalName::Unnamed || *stmt != StmtName::Unnamed => {
            Some(FrontendOp::Bind {
                portal: PortalName::Unnamed,
                stmt: StmtName::Unnamed,
                params: params.clone(),
            })
        }
        FrontendOp::DescribeStatement { name } if *name != StmtName::Unnamed => {
            Some(FrontendOp::DescribeStatement {
                name: StmtName::Unnamed,
            })
        }
        FrontendOp::DescribePortal { name } if *name != PortalName::Unnamed => {
            Some(FrontendOp::DescribePortal {
                name: PortalName::Unnamed,
            })
        }
        FrontendOp::Execute { portal, max_rows } if *portal != PortalName::Unnamed => {
            Some(FrontendOp::Execute {
                portal: PortalName::Unnamed,
                max_rows: *max_rows,
            })
        }
        FrontendOp::CloseStatement { name } if *name != StmtName::Unnamed => {
            Some(FrontendOp::CloseStatement {
                name: StmtName::Unnamed,
            })
        }
        FrontendOp::ClosePortal { name } if *name != PortalName::Unnamed => {
            Some(FrontendOp::ClosePortal {
                name: PortalName::Unnamed,
            })
        }
        _ => None,
    }
}

/// Try replacing SQL templates with `SELECT 1`.
async fn sql_simplification<F1: ConnectionFactory, F2: ConnectionFactory>(
    ops: &[FrontendOp],
    class: &DivergenceClass,
    runner: &DualRunner<F1, F2>,
) -> Option<Vec<FrontendOp>> {
    let mut current = ops.to_vec();
    let mut progress = false;

    for i in 0..current.len() {
        if let Some(simplified) = simplify_sql(&current[i]) {
            let mut candidate = current.clone();
            candidate[i] = simplified;
            if reproduces(&candidate, class, runner).await {
                current = candidate;
                progress = true;
            }
        }
    }

    if progress { Some(current) } else { None }
}

fn simplify_sql(op: &FrontendOp) -> Option<FrontendOp> {
    match op {
        FrontendOp::Query { sql } if sql != "SELECT 1" => Some(FrontendOp::Query {
            sql: "SELECT 1".into(),
        }),
        FrontendOp::Parse { name, sql, .. } if sql != "SELECT 1" => Some(FrontendOp::Parse {
            name: name.clone(),
            sql: "SELECT 1".into(),
            param_oids: vec![],
        }),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Fixed-point orchestrator
// ---------------------------------------------------------------------------

/// Run all shrink passes in a fixed-point loop until no pass makes progress.
/// Returns the minimized op sequence.
pub async fn shrink<F1: ConnectionFactory, F2: ConnectionFactory>(
    ops: &[FrontendOp],
    divergence: &Divergence,
    runner: &DualRunner<F1, F2>,
) -> Vec<FrontendOp> {
    let class = classify(divergence);
    let mut current = ops.to_vec();

    loop {
        let mut progress = false;

        if let Some(shrunk) = suffix_trim(&current, &class, runner).await {
            current = shrunk;
            progress = true;
        }
        if let Some(shrunk) = prefix_trim(&current, &class, runner).await {
            current = shrunk;
            progress = true;
        }
        if let Some(shrunk) = single_deletion(&current, &class, runner).await {
            current = shrunk;
            progress = true;
        }
        if let Some(shrunk) = subsequence(&current, &class, runner).await {
            current = shrunk;
            progress = true;
        }
        if let Some(shrunk) = name_simplification(&current, &class, runner).await {
            current = shrunk;
            progress = true;
        }
        if let Some(shrunk) = sql_simplification(&current, &class, runner).await {
            current = shrunk;
            progress = true;
        }

        if !progress {
            break;
        }
    }

    current
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock::{MockConnectionFactory, msg};
    use crate::op::Param;
    use crate::runner::DualRunner;
    use std::time::Duration;

    const TIMEOUT: Duration = Duration::from_millis(100);

    /// Create a DualRunner where pg returns success and target returns an error.
    fn divergent_runner() -> DualRunner<MockConnectionFactory, MockConnectionFactory> {
        let pg_bytes = msg::concat(&[
            msg::parse_complete(),
            msg::bind_complete(),
            msg::row_description(&[("?column?", 23)]),
            msg::data_row(&[Some(b"1")]),
            msg::command_complete("SELECT 1"),
            msg::ready_for_query(b'I'),
        ]);
        let target_bytes = msg::concat(&[
            msg::error_response("ERROR", "42601", "syntax error"),
            msg::ready_for_query(b'I'),
        ]);
        DualRunner::new(
            MockConnectionFactory {
                response_bytes: pg_bytes,
            },
            MockConnectionFactory {
                response_bytes: target_bytes,
            },
            TIMEOUT,
        )
    }

    fn sample_ops() -> Vec<FrontendOp> {
        vec![
            FrontendOp::Parse {
                name: StmtName::Unnamed,
                sql: "SELECT 1".into(),
                param_oids: vec![],
            },
            FrontendOp::Bind {
                portal: PortalName::Unnamed,
                stmt: StmtName::Unnamed,
                params: vec![],
            },
            FrontendOp::Execute {
                portal: PortalName::Unnamed,
                max_rows: 0,
            },
            FrontendOp::Sync,
        ]
    }

    fn sample_divergence() -> Divergence {
        Divergence {
            index: 0,
            expected: Some(NormalizedMsg::ParseComplete),
            actual: Some(NormalizedMsg::ErrorResponse {
                code: "42601".into(),
                message: "syntax error".into(),
            }),
            ops: sample_ops(),
        }
    }

    #[test]
    fn test_classify_same_class() {
        let div1 = Divergence {
            index: 0,
            expected: Some(NormalizedMsg::ParseComplete),
            actual: Some(NormalizedMsg::ErrorResponse {
                code: "42601".into(),
                message: "syntax error".into(),
            }),
            ops: vec![],
        };
        let div2 = Divergence {
            index: 3,
            expected: Some(NormalizedMsg::ParseComplete),
            actual: Some(NormalizedMsg::ErrorResponse {
                code: "42000".into(),
                message: "different error".into(),
            }),
            ops: vec![],
        };
        assert_eq!(classify(&div1), classify(&div2));
    }

    #[test]
    fn test_classify_different_class() {
        let div1 = Divergence {
            index: 0,
            expected: Some(NormalizedMsg::ParseComplete),
            actual: Some(NormalizedMsg::ErrorResponse {
                code: "42601".into(),
                message: "err".into(),
            }),
            ops: vec![],
        };
        let div2 = Divergence {
            index: 0,
            expected: Some(NormalizedMsg::BindComplete),
            actual: Some(NormalizedMsg::ErrorResponse {
                code: "42601".into(),
                message: "err".into(),
            }),
            ops: vec![],
        };
        assert_ne!(classify(&div1), classify(&div2));
    }

    #[test]
    fn test_classify_end_of_stream() {
        let div = Divergence {
            index: 0,
            expected: Some(NormalizedMsg::ParseComplete),
            actual: None,
            ops: vec![],
        };
        let class = classify(&div);
        assert_eq!(class.expected_kind, "ParseComplete");
        assert_eq!(class.actual_kind, "EndOfStream");
    }

    #[test]
    fn test_simplify_names_parse() {
        let op = FrontendOp::Parse {
            name: StmtName::S1,
            sql: "SELECT 1".into(),
            param_oids: vec![],
        };
        let simplified = simplify_names(&op).unwrap();
        assert!(matches!(
            simplified,
            FrontendOp::Parse {
                name: StmtName::Unnamed,
                ..
            }
        ));
    }

    #[test]
    fn test_simplify_names_already_unnamed() {
        let op = FrontendOp::Parse {
            name: StmtName::Unnamed,
            sql: "SELECT 1".into(),
            param_oids: vec![],
        };
        assert!(simplify_names(&op).is_none());
    }

    #[test]
    fn test_simplify_names_bind() {
        let op = FrontendOp::Bind {
            portal: PortalName::P1,
            stmt: StmtName::S2,
            params: vec![Param::Int32(1)],
        };
        let simplified = simplify_names(&op).unwrap();
        match simplified {
            FrontendOp::Bind {
                portal,
                stmt,
                params,
            } => {
                assert_eq!(portal, PortalName::Unnamed);
                assert_eq!(stmt, StmtName::Unnamed);
                assert_eq!(params, vec![Param::Int32(1)]);
            }
            _ => panic!("expected Bind"),
        }
    }

    #[test]
    fn test_simplify_names_execute() {
        let op = FrontendOp::Execute {
            portal: PortalName::P2,
            max_rows: 5,
        };
        let simplified = simplify_names(&op).unwrap();
        assert!(matches!(
            simplified,
            FrontendOp::Execute {
                portal: PortalName::Unnamed,
                max_rows: 5
            }
        ));
    }

    #[test]
    fn test_simplify_names_sync_unchanged() {
        assert!(simplify_names(&FrontendOp::Sync).is_none());
    }

    #[test]
    fn test_simplify_sql_query() {
        let op = FrontendOp::Query {
            sql: "SELECT * FROM pg_type".into(),
        };
        let simplified = simplify_sql(&op).unwrap();
        assert!(matches!(
            simplified,
            FrontendOp::Query { sql } if sql == "SELECT 1"
        ));
    }

    #[test]
    fn test_simplify_sql_already_simple() {
        let op = FrontendOp::Query {
            sql: "SELECT 1".into(),
        };
        assert!(simplify_sql(&op).is_none());
    }

    #[test]
    fn test_simplify_sql_parse() {
        let op = FrontendOp::Parse {
            name: StmtName::S1,
            sql: "SELECT $1::int".into(),
            param_oids: vec![23],
        };
        let simplified = simplify_sql(&op).unwrap();
        match simplified {
            FrontendOp::Parse {
                name,
                sql,
                param_oids,
            } => {
                assert_eq!(name, StmtName::S1); // name preserved
                assert_eq!(sql, "SELECT 1");
                assert!(param_oids.is_empty()); // params cleared
            }
            _ => panic!("expected Parse"),
        }
    }

    #[tokio::test]
    async fn test_shrink_reduces_sequence() {
        let runner = divergent_runner();
        let ops = sample_ops();
        let div = sample_divergence();

        let shrunk = shrink(&ops, &div, &runner).await;
        assert!(
            shrunk.len() < ops.len(),
            "shrunk ({}) should be shorter than original ({})",
            shrunk.len(),
            ops.len()
        );
    }

    #[tokio::test]
    async fn test_shrink_preserves_divergence() {
        let runner = divergent_runner();
        let ops = sample_ops();
        let div = sample_divergence();
        let class = classify(&div);

        let shrunk = shrink(&ops, &div, &runner).await;
        assert!(reproduces(&shrunk, &class, &runner).await);
    }

    #[tokio::test]
    async fn test_suffix_trim() {
        let runner = divergent_runner();
        let div = sample_divergence();
        let class = classify(&div);
        let ops = sample_ops();

        let trimmed = suffix_trim(&ops, &class, &runner).await.unwrap();
        assert!(trimmed.len() < ops.len());
    }

    #[tokio::test]
    async fn test_single_deletion() {
        let runner = divergent_runner();
        let div = sample_divergence();
        let class = classify(&div);
        let ops = sample_ops();

        let deleted = single_deletion(&ops, &class, &runner).await.unwrap();
        assert_eq!(deleted.len(), ops.len() - 1);
    }

    #[tokio::test]
    async fn test_shrink_with_named_ops() {
        let runner = divergent_runner();
        let ops = vec![
            FrontendOp::Parse {
                name: StmtName::S1,
                sql: "SELECT * FROM pg_type".into(),
                param_oids: vec![],
            },
            FrontendOp::Bind {
                portal: PortalName::P1,
                stmt: StmtName::S1,
                params: vec![],
            },
            FrontendOp::Execute {
                portal: PortalName::P1,
                max_rows: 0,
            },
            FrontendOp::Sync,
        ];
        let div = Divergence {
            index: 0,
            expected: Some(NormalizedMsg::ParseComplete),
            actual: Some(NormalizedMsg::ErrorResponse {
                code: "42601".into(),
                message: "syntax error".into(),
            }),
            ops: ops.clone(),
        };

        let shrunk = shrink(&ops, &div, &runner).await;
        assert!(shrunk.len() <= ops.len());
    }

    #[tokio::test]
    async fn test_shrink_single_op_sequence() {
        let runner = divergent_runner();
        let ops = vec![FrontendOp::Sync];
        let class = DivergenceClass {
            expected_kind: "ParseComplete".into(),
            actual_kind: "ErrorResponse".into(),
        };

        // Single op can't be shrunk further by deletion/trim passes
        assert!(suffix_trim(&ops, &class, &runner).await.is_none());
        assert!(prefix_trim(&ops, &class, &runner).await.is_none());
        assert!(single_deletion(&ops, &class, &runner).await.is_none());
    }
}
