use std::io;

use pg_stream::connection::PgConnection;
use pg_stream::startup::{AuthenticationMode, ConnectionBuilder};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

/// Creates ready-to-use PgConnection instances on demand.
/// Each fuzz iteration gets a fresh connection for state isolation.
pub trait ConnectionFactory {
    type Stream: AsyncRead + AsyncWrite + Unpin;

    fn connect(
        &self,
    ) -> impl std::future::Future<Output = io::Result<PgConnection<Self::Stream>>> + Send;
}

/// Creates connections to a real Postgres (or Postgres-compatible) server over TCP.
pub struct TcpConnectionFactory {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: Option<String>,
    pub database: String,
}

impl ConnectionFactory for TcpConnectionFactory {
    type Stream = TcpStream;

    async fn connect(&self) -> io::Result<PgConnection<TcpStream>> {
        let stream = TcpStream::connect((self.host.as_str(), self.port)).await?;
        let mut builder = ConnectionBuilder::new(&self.user).database(&self.database);
        if let Some(pw) = &self.password {
            builder = builder.auth(AuthenticationMode::Password(pw.clone()));
        }
        let (conn, _startup) = builder
            .connect(stream)
            .await
            .map_err(io::Error::other)?;
        Ok(conn)
    }
}
