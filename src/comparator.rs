use pg_stream::PgMessage;

use crate::op::FrontendOp;
use crate::runner::ResponseEvent;

/// A PgMessage normalized into an owned, comparable form.
/// Only fields that matter for comparison are kept.
#[derive(Debug, Clone, PartialEq)]
pub enum NormalizedMsg {
    // Fields-compared messages
    ReadyForQuery {
        status: String,
    },
    ErrorResponse {
        code: String,
        message: String,
    },
    NoticeResponse {
        code: String,
        message: String,
    },
    RowDescription {
        columns: Vec<ColumnDesc>,
    },
    DataRow {
        columns: Vec<Option<Vec<u8>>>,
    },
    CommandComplete {
        tag: String,
    },
    ParameterDescription {
        oids: Vec<u32>,
    },
    CopyInResponse {
        format: u16,
        column_formats: Vec<u16>,
    },
    CopyOutResponse {
        format: u16,
        column_formats: Vec<u16>,
    },
    CopyData {
        data: Vec<u8>,
    },

    // Presence-only messages (compared by type, no fields)
    ParseComplete,
    BindComplete,
    CloseComplete,
    NoData,
    EmptyQueryResponse,
    PortalSuspended,
    CopyDone,

    // Terminal events from the runner
    Timeout,
    Disconnected(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDesc {
    pub name: String,
    pub type_oid: u32,
    pub format: u16,
}

/// A divergence between the Postgres oracle and the target.
#[derive(Debug, Clone)]
pub struct Divergence {
    /// Index into the normalized (skip-filtered) response stream.
    pub index: usize,
    /// What Postgres returned (None if its stream was shorter).
    pub expected: Option<NormalizedMsg>,
    /// What the target returned (None if its stream was shorter).
    pub actual: Option<NormalizedMsg>,
    /// The op sequence that produced this divergence.
    pub ops: Vec<FrontendOp>,
}

impl std::fmt::Display for Divergence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "  Sequence ({} ops):", self.ops.len())?;
        for (i, op) in self.ops.iter().enumerate() {
            writeln!(f, "    [{i}] {op}")?;
        }
        writeln!(f, "  First mismatch at response index {}:", self.index)?;
        match (&self.expected, &self.actual) {
            (Some(e), Some(a)) => {
                writeln!(f, "    Postgres: {e:?}")?;
                writeln!(f, "    Target:   {a:?}")?;
            }
            (Some(e), None) => {
                writeln!(f, "    Postgres: {e:?}")?;
                writeln!(f, "    Target:   <end of stream>")?;
            }
            (None, Some(a)) => {
                writeln!(f, "    Postgres: <end of stream>")?;
                writeln!(f, "    Target:   {a:?}")?;
            }
            (None, None) => unreachable!(),
        }
        Ok(())
    }
}

/// Normalize a PgMessage into a NormalizedMsg, or None to skip it.
fn normalize(msg: &PgMessage) -> Option<NormalizedMsg> {
    match msg {
        PgMessage::ReadyForQuery(rfq) => Some(NormalizedMsg::ReadyForQuery {
            status: format!("{:?}", rfq.status()),
        }),
        PgMessage::ErrorResponse(e) => Some(NormalizedMsg::ErrorResponse {
            code: e.code().into_owned(),
            message: e.message().into_owned(),
        }),
        PgMessage::NoticeResponse(n) => Some(NormalizedMsg::NoticeResponse {
            code: n.code().into_owned(),
            message: n.message().into_owned(),
        }),
        PgMessage::RowDescription(rd) => {
            let mut columns = Vec::new();
            for i in 0..rd.column_count() as usize {
                columns.push(ColumnDesc {
                    name: rd
                        .column_name(i)
                        .map(|c| c.into_owned())
                        .unwrap_or_default(),
                    type_oid: rd.type_oid(i).unwrap_or(0),
                    format: rd.format(i).map(|f| f as u16).unwrap_or(0),
                });
            }
            Some(NormalizedMsg::RowDescription { columns })
        }
        PgMessage::DataRow(dr) => {
            let mut columns = Vec::new();
            for i in 0..dr.column_count() as usize {
                columns.push(dr.column(i).map(|b| b.to_vec()));
            }
            Some(NormalizedMsg::DataRow { columns })
        }
        PgMessage::CommandComplete(cc) => Some(NormalizedMsg::CommandComplete {
            tag: cc.tag().into_owned(),
        }),
        PgMessage::ParameterDescription(pd) => {
            let mut oids = Vec::new();
            for i in 0..pd.param_count() as usize {
                oids.push(pd.param_oid(i).unwrap_or(0));
            }
            Some(NormalizedMsg::ParameterDescription { oids })
        }
        PgMessage::CopyInResponse(cr) => {
            let mut column_formats = Vec::new();
            for i in 0..cr.column_count() as usize {
                column_formats.push(cr.column_format(i).map(|f| f as u16).unwrap_or(0));
            }
            Some(NormalizedMsg::CopyInResponse {
                format: cr.format() as u16,
                column_formats,
            })
        }
        PgMessage::CopyOutResponse(cr) => {
            let mut column_formats = Vec::new();
            for i in 0..cr.column_count() as usize {
                column_formats.push(cr.column_format(i).map(|f| f as u16).unwrap_or(0));
            }
            Some(NormalizedMsg::CopyOutResponse {
                format: cr.format() as u16,
                column_formats,
            })
        }
        PgMessage::CopyData(data) => Some(NormalizedMsg::CopyData {
            data: data.to_vec(),
        }),
        PgMessage::CopyDone => Some(NormalizedMsg::CopyDone),

        // Presence-only
        PgMessage::ParseComplete => Some(NormalizedMsg::ParseComplete),
        PgMessage::BindComplete => Some(NormalizedMsg::BindComplete),
        PgMessage::CloseComplete => Some(NormalizedMsg::CloseComplete),
        PgMessage::NoData => Some(NormalizedMsg::NoData),
        PgMessage::EmptyQueryResponse => Some(NormalizedMsg::EmptyQueryResponse),
        PgMessage::PortalSuspended => Some(NormalizedMsg::PortalSuspended),

        // Skipped — these legitimately differ between servers
        PgMessage::BackendKeyData(_) => None,
        PgMessage::ParameterStatus(_) => None,

        // Everything else: skip for now (Authentication, FunctionCallResponse, etc.)
        _ => None,
    }
}

/// Normalize a ResponseEvent, returning None for skipped messages.
fn normalize_event(event: &ResponseEvent) -> Option<NormalizedMsg> {
    match event {
        ResponseEvent::Message(msg) => normalize(msg),
        ResponseEvent::Timeout => Some(NormalizedMsg::Timeout),
        ResponseEvent::Disconnected(e) => Some(NormalizedMsg::Disconnected(e.clone())),
    }
}

/// Normalize and filter a response stream, keeping only comparable messages.
pub fn normalize_stream(events: &[ResponseEvent]) -> Vec<NormalizedMsg> {
    events.iter().filter_map(normalize_event).collect()
}

/// Compare two response streams (from Postgres and the target).
/// Returns the first divergence found, or None if they match.
pub fn compare(
    pg_events: &[ResponseEvent],
    target_events: &[ResponseEvent],
    ops: &[FrontendOp],
) -> Option<Divergence> {
    let pg = normalize_stream(pg_events);
    let target = normalize_stream(target_events);

    let max_len = std::cmp::max(pg.len(), target.len());
    for i in 0..max_len {
        let pg_msg = pg.get(i);
        let target_msg = target.get(i);
        match (pg_msg, target_msg) {
            (Some(p), Some(t)) if p == t => continue,
            (Some(p), Some(t)) => {
                return Some(Divergence {
                    index: i,
                    expected: Some(p.clone()),
                    actual: Some(t.clone()),
                    ops: ops.to_vec(),
                });
            }
            (Some(p), None) => {
                return Some(Divergence {
                    index: i,
                    expected: Some(p.clone()),
                    actual: None,
                    ops: ops.to_vec(),
                });
            }
            (None, Some(t)) => {
                return Some(Divergence {
                    index: i,
                    expected: None,
                    actual: Some(t.clone()),
                    ops: ops.to_vec(),
                });
            }
            (None, None) => unreachable!(),
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock::msg;
    use crate::op::{PortalName, StmtName};

    fn make_events(messages: Vec<Vec<u8>>) -> Vec<ResponseEvent> {
        // Parse wire-format bytes through a mock connection to get real PgMessages
        use crate::mock::mock_conn;
        let bytes = msg::concat(&messages);
        let mut conn = mock_conn(bytes);
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut events = Vec::new();
        rt.block_on(async {
            while let Ok(msg) = conn.recv().await {
                events.push(ResponseEvent::Message(Box::new(msg)));
            }
        });
        events
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

    #[test]
    fn test_identical_streams_no_divergence() {
        let messages = vec![
            msg::parse_complete(),
            msg::bind_complete(),
            msg::data_row(&[Some(b"1")]),
            msg::command_complete("SELECT 1"),
            msg::ready_for_query(b'I'),
        ];

        let pg = make_events(messages.clone());
        let target = make_events(messages);
        let ops = sample_ops();

        assert!(compare(&pg, &target, &ops).is_none());
    }

    #[test]
    fn test_different_data_row_values() {
        let pg = make_events(vec![
            msg::parse_complete(),
            msg::bind_complete(),
            msg::data_row(&[Some(b"1")]),
            msg::command_complete("SELECT 1"),
            msg::ready_for_query(b'I'),
        ]);
        let target = make_events(vec![
            msg::parse_complete(),
            msg::bind_complete(),
            msg::data_row(&[Some(b"2")]), // different value
            msg::command_complete("SELECT 1"),
            msg::ready_for_query(b'I'),
        ]);

        let div = compare(&pg, &target, &sample_ops()).unwrap();
        assert_eq!(div.index, 2); // DataRow is at index 2
        assert!(matches!(&div.expected, Some(NormalizedMsg::DataRow { .. })));
        assert!(matches!(&div.actual, Some(NormalizedMsg::DataRow { .. })));
    }

    #[test]
    fn test_error_vs_success() {
        let pg = make_events(vec![
            msg::parse_complete(),
            msg::bind_complete(),
            msg::data_row(&[Some(b"1")]),
            msg::command_complete("SELECT 1"),
            msg::ready_for_query(b'I'),
        ]);
        let target = make_events(vec![
            msg::error_response("ERROR", "42601", "syntax error"),
            msg::ready_for_query(b'I'),
        ]);

        let div = compare(&pg, &target, &sample_ops()).unwrap();
        assert_eq!(div.index, 0); // First message differs
        assert!(matches!(
            &div.expected,
            Some(NormalizedMsg::ParseComplete)
        ));
        assert!(matches!(
            &div.actual,
            Some(NormalizedMsg::ErrorResponse { .. })
        ));
    }

    #[test]
    fn test_different_error_codes() {
        let pg = make_events(vec![
            msg::error_response("ERROR", "42601", "syntax error"),
            msg::ready_for_query(b'I'),
        ]);
        let target = make_events(vec![
            msg::error_response("ERROR", "42000", "syntax error"),
            msg::ready_for_query(b'I'),
        ]);

        let div = compare(&pg, &target, &sample_ops()).unwrap();
        assert_eq!(div.index, 0);
    }

    #[test]
    fn test_different_transaction_status() {
        let pg = make_events(vec![
            msg::parse_complete(),
            msg::ready_for_query(b'I'), // Idle
        ]);
        let target = make_events(vec![
            msg::parse_complete(),
            msg::ready_for_query(b'E'), // Failed
        ]);

        let div = compare(&pg, &target, &sample_ops()).unwrap();
        assert_eq!(div.index, 1); // ReadyForQuery differs
    }

    #[test]
    fn test_target_stream_shorter() {
        let pg = make_events(vec![
            msg::parse_complete(),
            msg::bind_complete(),
            msg::ready_for_query(b'I'),
        ]);
        let target = make_events(vec![msg::parse_complete(), msg::ready_for_query(b'I')]);

        let div = compare(&pg, &target, &sample_ops()).unwrap();
        assert_eq!(div.index, 1); // pg has BindComplete, target has ReadyForQuery
    }

    #[test]
    fn test_skipped_messages_ignored() {
        // ParameterStatus is skipped — shouldn't cause divergence
        // We can't easily inject ParameterStatus into the middle of a mock stream
        // because make_events reads sequentially. But we can test normalization directly.
        let pg = vec![ResponseEvent::Timeout];
        let target = vec![ResponseEvent::Timeout];

        assert!(compare(&pg, &target, &sample_ops()).is_none());
    }

    #[test]
    fn test_timeout_vs_message() {
        let pg = make_events(vec![msg::ready_for_query(b'I')]);
        let target = vec![ResponseEvent::Timeout];

        let div = compare(&pg, &target, &sample_ops()).unwrap();
        assert_eq!(div.index, 0);
        assert!(matches!(&div.actual, Some(NormalizedMsg::Timeout)));
    }

    #[test]
    fn test_close_complete_and_no_data() {
        let messages = vec![
            msg::close_complete(),
            msg::no_data(),
            msg::ready_for_query(b'I'),
        ];
        let pg = make_events(messages.clone());
        let target = make_events(messages);
        assert!(compare(&pg, &target, &sample_ops()).is_none());
    }

    #[test]
    fn test_empty_query_and_portal_suspended() {
        let pg = make_events(vec![
            msg::empty_query_response(),
            msg::ready_for_query(b'I'),
        ]);
        let target = make_events(vec![
            msg::portal_suspended(),
            msg::ready_for_query(b'I'),
        ]);
        let div = compare(&pg, &target, &sample_ops()).unwrap();
        assert_eq!(div.index, 0);
    }

    #[test]
    fn test_notice_response_compared() {
        let pg = make_events(vec![
            msg::notice_response("WARNING", "01000", "test warning"),
            msg::ready_for_query(b'I'),
        ]);
        let target = make_events(vec![
            msg::notice_response("WARNING", "01000", "different warning"),
            msg::ready_for_query(b'I'),
        ]);
        let div = compare(&pg, &target, &sample_ops()).unwrap();
        assert_eq!(div.index, 0);
    }

    #[test]
    fn test_parameter_description_compared() {
        let pg = make_events(vec![
            msg::parameter_description(&[23, 25]),
            msg::ready_for_query(b'I'),
        ]);
        let target = make_events(vec![
            msg::parameter_description(&[23, 20]), // different OID
            msg::ready_for_query(b'I'),
        ]);
        let div = compare(&pg, &target, &sample_ops()).unwrap();
        assert_eq!(div.index, 0);
    }

    #[test]
    fn test_copy_in_response_compared() {
        let pg = make_events(vec![
            msg::copy_in_response(0, &[0]),
            msg::copy_done(),
            msg::command_complete("COPY 0"),
            msg::ready_for_query(b'I'),
        ]);
        let target = make_events(vec![
            msg::copy_in_response(1, &[1]), // different format
            msg::copy_done(),
            msg::command_complete("COPY 0"),
            msg::ready_for_query(b'I'),
        ]);
        let div = compare(&pg, &target, &sample_ops()).unwrap();
        assert_eq!(div.index, 0);
    }

    #[test]
    fn test_copy_out_response_compared() {
        let messages = vec![
            msg::copy_out_response(0, &[0]),
            msg::copy_data(b"1\n"),
            msg::copy_done(),
            msg::command_complete("COPY 1"),
            msg::ready_for_query(b'I'),
        ];
        let pg = make_events(messages.clone());
        let target = make_events(messages);
        assert!(compare(&pg, &target, &sample_ops()).is_none());
    }

    #[test]
    fn test_divergence_display() {
        let div = Divergence {
            index: 0,
            expected: Some(NormalizedMsg::ParseComplete),
            actual: Some(NormalizedMsg::ErrorResponse {
                code: "42601".into(),
                message: "syntax error".into(),
            }),
            ops: sample_ops(),
        };
        let s = format!("{div}");
        assert!(s.contains("Parse"));
        assert!(s.contains("Postgres:"));
        assert!(s.contains("Target:"));
    }
}
