use std::fmt;

use bytes::BufMut;
use pg_stream::message::{Bindable, FormatCode};

pub type Oid = u32;

/// A prepared statement name. Mirrors pg_stream's `Option<&str>` convention:
/// `Unnamed` maps to `None` (the unnamed statement), named variants map to `Some("s1")` etc.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum StmtName {
    Unnamed,
    S1,
    S2,
}

impl StmtName {
    pub const POOL: &[StmtName] = &[StmtName::Unnamed, StmtName::S1, StmtName::S2];

    pub fn as_option(&self) -> Option<&str> {
        match self {
            StmtName::Unnamed => None,
            StmtName::S1 => Some("s1"),
            StmtName::S2 => Some("s2"),
        }
    }

    /// Returns the name as a plain &str. Unnamed = "".
    /// Needed by BindBuilder::statement() which takes &str, not Option<&str>.
    pub fn as_str(&self) -> &str {
        self.as_option().unwrap_or("")
    }
}

impl fmt::Display for StmtName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StmtName::Unnamed => write!(f, "\"\""),
            StmtName::S1 => write!(f, "\"s1\""),
            StmtName::S2 => write!(f, "\"s2\""),
        }
    }
}

/// A portal name. Same convention as `StmtName`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PortalName {
    Unnamed,
    P1,
    P2,
}

impl PortalName {
    pub const POOL: &[PortalName] = &[PortalName::Unnamed, PortalName::P1, PortalName::P2];

    pub fn as_option(&self) -> Option<&str> {
        match self {
            PortalName::Unnamed => None,
            PortalName::P1 => Some("p1"),
            PortalName::P2 => Some("p2"),
        }
    }
}

impl fmt::Display for PortalName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PortalName::Unnamed => write!(f, "\"\""),
            PortalName::P1 => write!(f, "\"p1\""),
            PortalName::P2 => write!(f, "\"p2\""),
        }
    }
}

/// A bind parameter value. Kept simple — we're fuzzing protocol sequences, not SQL types.
#[derive(Debug, Clone, PartialEq)]
pub enum Param {
    Null,
    Int32(i32),
    Text(String),
    Bytes(Vec<u8>),
}

impl Bindable for Param {
    fn format_code(&self) -> FormatCode {
        match self {
            Param::Null | Param::Int32(_) | Param::Bytes(_) => FormatCode::Binary,
            Param::Text(_) => FormatCode::Text,
        }
    }

    fn encode(&self, buf: &mut dyn BufMut) {
        match self {
            Param::Null => buf.put_i32(-1),
            Param::Int32(v) => {
                buf.put_i32(4);
                buf.put_i32(*v);
            }
            Param::Text(s) => {
                buf.put_i32(s.len() as i32);
                buf.put_slice(s.as_bytes());
            }
            Param::Bytes(b) => {
                buf.put_i32(b.len() as i32);
                buf.put_slice(b);
            }
        }
    }
}

impl fmt::Display for Param {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Param::Null => write!(f, "NULL"),
            Param::Int32(v) => write!(f, "{v}"),
            Param::Text(s) => write!(f, "'{s}'"),
            Param::Bytes(b) => write!(f, "\\x{}", hex(b)),
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// A single frontend protocol operation. Each variant maps to one or more wire messages
/// sent to the server via pg_stream's `PgProtocol` trait.
#[derive(Debug, Clone, PartialEq)]
pub enum FrontendOp {
    // Simple query protocol
    Query {
        sql: String,
    },

    // Extended query protocol
    Parse {
        name: StmtName,
        sql: String,
        param_oids: Vec<Oid>,
    },
    Bind {
        portal: PortalName,
        stmt: StmtName,
        params: Vec<Param>,
    },
    DescribeStatement {
        name: StmtName,
    },
    DescribePortal {
        name: PortalName,
    },
    Execute {
        portal: PortalName,
        max_rows: u32,
    },
    CloseStatement {
        name: StmtName,
    },
    ClosePortal {
        name: PortalName,
    },
    Sync,
    Flush,

    // COPY sub-protocol
    CopyData {
        data: Vec<u8>,
    },
    CopyDone,
    CopyFail {
        message: String,
    },

    // Other
    Terminate,
}

impl fmt::Display for FrontendOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FrontendOp::Query { sql } => write!(f, "Query {{ sql: \"{sql}\" }}"),
            FrontendOp::Parse {
                name,
                sql,
                param_oids,
            } => {
                write!(f, "Parse {{ name: {name}, sql: \"{sql}\"")?;
                if !param_oids.is_empty() {
                    write!(f, ", param_oids: {param_oids:?}")?;
                }
                write!(f, " }}")
            }
            FrontendOp::Bind {
                portal,
                stmt,
                params,
            } => {
                write!(f, "Bind {{ portal: {portal}, stmt: {stmt}")?;
                if !params.is_empty() {
                    write!(f, ", params: [")?;
                    for (i, p) in params.iter().enumerate() {
                        if i > 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "{p}")?;
                    }
                    write!(f, "]")?;
                }
                write!(f, " }}")
            }
            FrontendOp::DescribeStatement { name } => {
                write!(f, "DescribeStatement {{ name: {name} }}")
            }
            FrontendOp::DescribePortal { name } => {
                write!(f, "DescribePortal {{ name: {name} }}")
            }
            FrontendOp::Execute { portal, max_rows } => {
                write!(f, "Execute {{ portal: {portal}, max_rows: {max_rows} }}")
            }
            FrontendOp::CloseStatement { name } => {
                write!(f, "CloseStatement {{ name: {name} }}")
            }
            FrontendOp::ClosePortal { name } => {
                write!(f, "ClosePortal {{ name: {name} }}")
            }
            FrontendOp::Sync => write!(f, "Sync"),
            FrontendOp::Flush => write!(f, "Flush"),
            FrontendOp::CopyData { data } => {
                write!(f, "CopyData {{ {} bytes }}", data.len())
            }
            FrontendOp::CopyDone => write!(f, "CopyDone"),
            FrontendOp::CopyFail { message } => {
                write!(f, "CopyFail {{ \"{message}\" }}")
            }
            FrontendOp::Terminate => write!(f, "Terminate"),
        }
    }
}

/// Returns true if this op triggers an immediate TCP flush (the server will
/// process buffered messages and respond).
impl FrontendOp {
    pub fn triggers_flush(&self) -> bool {
        matches!(
            self,
            FrontendOp::Sync
                | FrontendOp::Query { .. }
                | FrontendOp::Flush
                | FrontendOp::CopyDone
                | FrontendOp::CopyFail { .. }
                | FrontendOp::Terminate
        )
    }
}
