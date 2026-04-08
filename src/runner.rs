use std::io;
use std::time::Duration;

use pg_stream::PgMessage;
use pg_stream::connection::PgConnection;
use pg_stream::message::{Bindable, PgProtocol};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::connection::ConnectionFactory;
use crate::op::FrontendOp;

/// What we record from each server's response stream.
#[derive(Debug, Clone)]
pub enum ResponseEvent {
    /// A normal protocol message from the server.
    Message(Box<PgMessage>),
    /// No response within the timeout window.
    Timeout,
    /// Connection closed or I/O error.
    Disconnected(String),
}

/// Encode a single FrontendOp into the connection's write buffer.
/// Does NOT flush — the caller decides when to flush based on `triggers_flush()`.
pub fn encode_op<S: AsyncRead + AsyncWrite + Unpin>(conn: &mut PgConnection<S>, op: &FrontendOp) {
    match op {
        FrontendOp::Query { sql } => {
            conn.query(sql);
        }
        FrontendOp::Parse {
            name,
            sql,
            param_oids,
        } => {
            let builder = conn.parse(name.as_option()).query(sql);
            if param_oids.is_empty() {
                builder.finish();
            } else {
                builder.param_types(param_oids).finish();
            }
        }
        FrontendOp::Bind {
            portal,
            stmt,
            params,
        } => {
            let param_refs: Vec<&dyn Bindable> =
                params.iter().map(|p| p as &dyn Bindable).collect();
            conn.bind(portal.as_option())
                .statement(stmt.as_str())
                .finish(&param_refs);
        }
        FrontendOp::DescribeStatement { name } => {
            conn.describe_statement(name.as_option());
        }
        FrontendOp::DescribePortal { name } => {
            conn.describe_portal(name.as_option());
        }
        FrontendOp::Execute { portal, max_rows } => {
            conn.execute(portal.as_option(), *max_rows);
        }
        FrontendOp::CloseStatement { name } => {
            conn.close_statement(name.as_option());
        }
        FrontendOp::ClosePortal { name } => {
            conn.close_portal(name.as_option());
        }
        FrontendOp::Sync => {
            conn.sync();
        }
        FrontendOp::Flush => {
            conn.flush_msg();
        }
        FrontendOp::CopyData { data } => {
            conn.copy_data(data);
        }
        FrontendOp::CopyDone => {
            conn.copy_done();
        }
        FrontendOp::CopyFail { message } => {
            conn.copy_fail(message);
        }
        FrontendOp::Terminate => {
            conn.terminate();
        }
    }
}

/// Run a sequence of FrontendOps against a connection, respecting flush points.
/// Returns the full response stream collected from the server.
pub async fn run_sequence<S: AsyncRead + AsyncWrite + Unpin>(
    conn: &mut PgConnection<S>,
    ops: &[FrontendOp],
    timeout: Duration,
) -> Vec<ResponseEvent> {
    // Count expected ReadyForQuery messages. Each Sync and each Query produces exactly one.
    let expected_rfq = ops
        .iter()
        .filter(|op| matches!(op, FrontendOp::Sync | FrontendOp::Query { .. }))
        .count();

    let has_terminate = ops.iter().any(|op| matches!(op, FrontendOp::Terminate));

    // Encode all ops, flushing the TCP stream at each flush point.
    for op in ops {
        encode_op(conn, op);
        if op.triggers_flush()
            && let Err(e) = conn.flush().await
        {
            return vec![ResponseEvent::Disconnected(e.to_string())];
        }
    }

    // If nothing triggered a flush (e.g., only Parse/Bind with no Sync), force one
    // so the server sees something. This is a degenerate case but we should handle it.
    if !ops.iter().any(|op| op.triggers_flush())
        && let Err(e) = conn.flush().await
    {
        return vec![ResponseEvent::Disconnected(e.to_string())];
    }

    // Collect responses.
    collect_responses(conn, expected_rfq, has_terminate, timeout).await
}

/// Read response messages from the connection until we've seen the expected number
/// of ReadyForQuery messages, or we hit a timeout / disconnect.
async fn collect_responses<S: AsyncRead + AsyncWrite + Unpin>(
    conn: &mut PgConnection<S>,
    expected_rfq: usize,
    has_terminate: bool,
    timeout: Duration,
) -> Vec<ResponseEvent> {
    let mut responses = Vec::new();
    let mut rfq_count = 0;

    // If the sequence had a Terminate, the server will close the connection.
    // If there are no Sync/Query ops, there's no ReadyForQuery to wait for —
    // just drain whatever arrives within the timeout.
    let expecting_rfq = expected_rfq > 0 && !has_terminate;

    loop {
        match tokio::time::timeout(timeout, conn.recv()).await {
            Ok(Ok(msg)) => {
                if matches!(&msg, PgMessage::ReadyForQuery(_)) {
                    rfq_count += 1;
                }
                responses.push(ResponseEvent::Message(Box::new(msg)));
                if expecting_rfq && rfq_count >= expected_rfq {
                    break;
                }
            }
            Ok(Err(e)) => {
                // Connection closed or I/O error. If we already have some responses
                // this might be expected (e.g., after Terminate).
                if !has_terminate || responses.is_empty() {
                    responses.push(ResponseEvent::Disconnected(e.to_string()));
                }
                break;
            }
            Err(_) => {
                // Timeout. If we were expecting more ReadyForQuery, record it.
                if expecting_rfq && rfq_count < expected_rfq {
                    responses.push(ResponseEvent::Timeout);
                }
                break;
            }
        }
    }

    responses
}

/// Run setup SQL statements on a connection using simple query protocol.
/// Each statement gets its own Query + ReadyForQuery cycle.
async fn run_setup<S: AsyncRead + AsyncWrite + Unpin>(
    conn: &mut PgConnection<S>,
    setup_stmts: &[&str],
    timeout: Duration,
) -> io::Result<()> {
    for stmt in setup_stmts {
        conn.query(stmt);
        conn.flush().await?;
        // Drain until ReadyForQuery
        loop {
            match tokio::time::timeout(timeout, conn.recv()).await {
                Ok(Ok(PgMessage::ReadyForQuery(_))) => break,
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(e),
                Err(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!("timeout during setup: {stmt}"),
                    ));
                }
            }
        }
    }
    Ok(())
}

/// Runs the same op sequence against two connections and returns both response streams.
pub struct DualRunner<F1: ConnectionFactory, F2: ConnectionFactory> {
    pub pg_factory: F1,
    pub target_factory: F2,
    pub timeout: Duration,
    pub setup_stmts: Vec<String>,
}

impl<F1: ConnectionFactory, F2: ConnectionFactory> DualRunner<F1, F2> {
    pub fn new(pg_factory: F1, target_factory: F2, timeout: Duration) -> Self {
        Self {
            pg_factory,
            target_factory,
            timeout,
            setup_stmts: Vec::new(),
        }
    }

    /// Set setup SQL to run on each fresh connection pair before fuzzing.
    pub fn with_setup(mut self, stmts: Vec<String>) -> Self {
        self.setup_stmts = stmts;
        self
    }

    /// Open fresh connections, run setup, replay ops, return both response streams.
    pub async fn run(
        &self,
        ops: &[FrontendOp],
    ) -> io::Result<(Vec<ResponseEvent>, Vec<ResponseEvent>)> {
        let mut pg_conn = self.pg_factory.connect().await?;
        let mut target_conn = self.target_factory.connect().await?;

        // Run setup SQL on both connections.
        let setup_refs: Vec<&str> = self.setup_stmts.iter().map(|s| s.as_str()).collect();
        if !setup_refs.is_empty() {
            run_setup(&mut pg_conn, &setup_refs, self.timeout).await?;
            run_setup(&mut target_conn, &setup_refs, self.timeout).await?;
        }

        // Replay the same ops against both.
        let pg_responses = run_sequence(&mut pg_conn, ops, self.timeout).await;
        let target_responses = run_sequence(&mut target_conn, ops, self.timeout).await;

        Ok((pg_responses, target_responses))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock::{mock_conn, msg};
    use crate::op::{Param, PortalName, StmtName};

    const TIMEOUT: Duration = Duration::from_millis(100);

    /// Helper: assert the response stream contains exactly these message types in order.
    fn assert_message_types(responses: &[ResponseEvent], expected: &[&str]) {
        let actual: Vec<String> = responses
            .iter()
            .map(|r| match r {
                ResponseEvent::Message(msg) => format!("{msg:?}")
                    .split('(')
                    .next()
                    .unwrap_or("?")
                    .to_string(),
                ResponseEvent::Timeout => "Timeout".to_string(),
                ResponseEvent::Disconnected(_) => "Disconnected".to_string(),
            })
            .collect();
        let expected: Vec<String> = expected.iter().map(|s| s.to_string()).collect();
        assert_eq!(actual, expected, "message types mismatch");
    }

    #[tokio::test]
    async fn test_happy_path_extended_query() {
        // Parse/Bind/Execute/Sync → ParseComplete, BindComplete, DataRow, CommandComplete, RFQ
        let response_bytes = msg::concat(&[
            msg::parse_complete(),
            msg::bind_complete(),
            msg::row_description(&[("?column?", 23)]), // int4
            msg::data_row(&[Some(b"1")]),
            msg::command_complete("SELECT 1"),
            msg::ready_for_query(b'I'),
        ]);

        let mut conn = mock_conn(response_bytes);
        let ops = vec![
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
        ];

        let responses = run_sequence(&mut conn, &ops, TIMEOUT).await;

        assert_eq!(responses.len(), 6);
        assert_message_types(
            &responses,
            &[
                "ParseComplete",
                "BindComplete",
                "RowDescription",
                "DataRow",
                "CommandComplete",
                "ReadyForQuery",
            ],
        );
    }

    #[tokio::test]
    async fn test_simple_query() {
        let response_bytes = msg::concat(&[
            msg::row_description(&[("?column?", 25)]), // text
            msg::data_row(&[Some(b"hello")]),
            msg::command_complete("SELECT 1"),
            msg::ready_for_query(b'I'),
        ]);

        let mut conn = mock_conn(response_bytes);
        let ops = vec![FrontendOp::Query {
            sql: "SELECT 'hello'".into(),
        }];

        let responses = run_sequence(&mut conn, &ops, TIMEOUT).await;

        assert_eq!(responses.len(), 4);
        assert_message_types(
            &responses,
            &[
                "RowDescription",
                "DataRow",
                "CommandComplete",
                "ReadyForQuery",
            ],
        );
    }

    #[tokio::test]
    async fn test_parse_error() {
        // Parse with bad SQL → ErrorResponse, then Sync → ReadyForQuery
        let response_bytes = msg::concat(&[
            msg::error_response("ERROR", "42601", "syntax error at or near \"SLECT\""),
            msg::ready_for_query(b'I'),
        ]);

        let mut conn = mock_conn(response_bytes);
        let ops = vec![
            FrontendOp::Parse {
                name: StmtName::Unnamed,
                sql: "SLECT 1".into(),
                param_oids: vec![],
            },
            FrontendOp::Sync,
        ];

        let responses = run_sequence(&mut conn, &ops, TIMEOUT).await;

        assert_eq!(responses.len(), 2);
        assert_message_types(&responses, &["ErrorResponse", "ReadyForQuery"]);
    }

    #[tokio::test]
    async fn test_parameterized_query() {
        let response_bytes = msg::concat(&[
            msg::parse_complete(),
            msg::bind_complete(),
            msg::row_description(&[("?column?", 23)]),
            msg::data_row(&[Some(b"42")]),
            msg::command_complete("SELECT 1"),
            msg::ready_for_query(b'I'),
        ]);

        let mut conn = mock_conn(response_bytes);
        let ops = vec![
            FrontendOp::Parse {
                name: StmtName::Unnamed,
                sql: "SELECT $1::int".into(),
                param_oids: vec![],
            },
            FrontendOp::Bind {
                portal: PortalName::Unnamed,
                stmt: StmtName::Unnamed,
                params: vec![Param::Int32(42)],
            },
            FrontendOp::Execute {
                portal: PortalName::Unnamed,
                max_rows: 0,
            },
            FrontendOp::Sync,
        ];

        let responses = run_sequence(&mut conn, &ops, TIMEOUT).await;

        assert_eq!(responses.len(), 6);
        assert_message_types(
            &responses,
            &[
                "ParseComplete",
                "BindComplete",
                "RowDescription",
                "DataRow",
                "CommandComplete",
                "ReadyForQuery",
            ],
        );
    }

    #[tokio::test]
    async fn test_multiple_syncs() {
        // Two extended query cycles in one sequence
        let response_bytes = msg::concat(&[
            msg::parse_complete(),
            msg::bind_complete(),
            msg::data_row(&[Some(b"1")]),
            msg::command_complete("SELECT 1"),
            msg::ready_for_query(b'I'),
            msg::parse_complete(),
            msg::bind_complete(),
            msg::data_row(&[Some(b"2")]),
            msg::command_complete("SELECT 1"),
            msg::ready_for_query(b'I'),
        ]);

        let mut conn = mock_conn(response_bytes);
        let ops = vec![
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
            FrontendOp::Parse {
                name: StmtName::Unnamed,
                sql: "SELECT 2".into(),
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
        ];

        let responses = run_sequence(&mut conn, &ops, TIMEOUT).await;

        // Two full cycles = 10 messages
        assert_eq!(responses.len(), 10);
        // Should end with two ReadyForQuery
        let rfq_count = responses
            .iter()
            .filter(|r| matches!(r, ResponseEvent::Message(msg) if matches!(msg.as_ref(), PgMessage::ReadyForQuery(_))))
            .count();
        assert_eq!(rfq_count, 2);
    }

    #[tokio::test]
    async fn test_disconnect_on_eof() {
        // Mock returns nothing — EOF immediately
        let mut conn = mock_conn(vec![]);
        let ops = vec![FrontendOp::Sync];

        let responses = run_sequence(&mut conn, &ops, TIMEOUT).await;

        assert_eq!(responses.len(), 1);
        assert!(matches!(&responses[0], ResponseEvent::Disconnected(_)));
    }

    #[tokio::test]
    async fn test_no_flush_points() {
        // Only Parse/Bind with no Sync — no ReadyForQuery expected.
        // Mock returns nothing, so we should get a timeout after the forced flush.
        let mut conn = mock_conn(vec![]);
        let ops = vec![
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
        ];

        let responses = run_sequence(&mut conn, &ops, TIMEOUT).await;

        // No Sync/Query means expected_rfq = 0, so we don't expect ReadyForQuery.
        // The mock EOF will cause a disconnect during the drain.
        // With no expected RFQ and a has_terminate=false, the collector drains until
        // EOF or timeout. EOF on empty mock → Disconnected.
        assert!(responses.len() <= 1);
    }

    #[tokio::test]
    async fn test_named_statement_and_portal() {
        let response_bytes = msg::concat(&[
            msg::parse_complete(),
            msg::bind_complete(),
            msg::data_row(&[Some(b"1")]),
            msg::command_complete("SELECT 1"),
            msg::ready_for_query(b'I'),
        ]);

        let mut conn = mock_conn(response_bytes);
        let ops = vec![
            FrontendOp::Parse {
                name: StmtName::S1,
                sql: "SELECT 1".into(),
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

        let responses = run_sequence(&mut conn, &ops, TIMEOUT).await;

        assert_eq!(responses.len(), 5);
        assert_message_types(
            &responses,
            &[
                "ParseComplete",
                "BindComplete",
                "DataRow",
                "CommandComplete",
                "ReadyForQuery",
            ],
        );
    }

    #[tokio::test]
    async fn test_dual_runner_identical_responses() {
        use crate::mock::MockConnectionFactory;

        let response_bytes = msg::concat(&[
            msg::parse_complete(),
            msg::bind_complete(),
            msg::data_row(&[Some(b"1")]),
            msg::command_complete("SELECT 1"),
            msg::ready_for_query(b'I'),
        ]);

        let runner = DualRunner::new(
            MockConnectionFactory {
                response_bytes: response_bytes.clone(),
            },
            MockConnectionFactory { response_bytes },
            TIMEOUT,
        );

        let ops = vec![
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
        ];

        let (pg, target) = runner.run(&ops).await.unwrap();

        assert_eq!(pg.len(), 5);
        assert_eq!(target.len(), 5);
        assert_message_types(
            &pg,
            &[
                "ParseComplete",
                "BindComplete",
                "DataRow",
                "CommandComplete",
                "ReadyForQuery",
            ],
        );
        assert_message_types(
            &target,
            &[
                "ParseComplete",
                "BindComplete",
                "DataRow",
                "CommandComplete",
                "ReadyForQuery",
            ],
        );
    }

    #[tokio::test]
    async fn test_dual_runner_divergent_responses() {
        use crate::mock::MockConnectionFactory;

        // PG returns success
        let pg_bytes = msg::concat(&[
            msg::parse_complete(),
            msg::bind_complete(),
            msg::data_row(&[Some(b"1")]),
            msg::command_complete("SELECT 1"),
            msg::ready_for_query(b'I'),
        ]);

        // Target returns error after parse
        let target_bytes = msg::concat(&[
            msg::error_response("ERROR", "42601", "syntax error"),
            msg::ready_for_query(b'I'),
        ]);

        let runner = DualRunner::new(
            MockConnectionFactory {
                response_bytes: pg_bytes,
            },
            MockConnectionFactory {
                response_bytes: target_bytes,
            },
            TIMEOUT,
        );

        let ops = vec![
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
        ];

        let (pg, target) = runner.run(&ops).await.unwrap();

        // Different lengths — the comparator will catch this
        assert_eq!(pg.len(), 5);
        assert_eq!(target.len(), 2);
        assert_message_types(&pg[..1], &["ParseComplete"]);
        assert_message_types(&target[..1], &["ErrorResponse"]);
    }

    #[tokio::test]
    async fn test_dual_runner_with_setup() {
        use crate::mock::MockConnectionFactory;

        // Setup produces: CommandComplete + ReadyForQuery
        // Then the real query produces the usual
        let response_bytes = msg::concat(&[
            // setup response
            msg::command_complete("CREATE TABLE"),
            msg::ready_for_query(b'I'),
            // actual query response
            msg::parse_complete(),
            msg::bind_complete(),
            msg::data_row(&[Some(b"1")]),
            msg::command_complete("SELECT 1"),
            msg::ready_for_query(b'I'),
        ]);

        let runner = DualRunner::new(
            MockConnectionFactory {
                response_bytes: response_bytes.clone(),
            },
            MockConnectionFactory { response_bytes },
            TIMEOUT,
        )
        .with_setup(vec!["CREATE TABLE IF NOT EXISTS copy_test (id int)".into()]);

        let ops = vec![
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
        ];

        let (pg, target) = runner.run(&ops).await.unwrap();

        // Setup responses are consumed; only the query responses remain
        assert_eq!(pg.len(), 5);
        assert_eq!(target.len(), 5);
    }
}
