use std::io;
use std::sync::Arc;

use pg_stream::connection::PgConnection;
use pg_stream::startup::{AuthenticationMode, ConnectionBuilder};
use rustls::ClientConfig;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

/// Creates ready-to-use PgConnection instances on demand.
/// Each fuzz iteration gets a fresh connection for state isolation.
pub trait ConnectionFactory {
    type Stream: AsyncRead + AsyncWrite + Unpin;

    fn connect(
        &self,
    ) -> impl std::future::Future<Output = io::Result<PgConnection<Self::Stream>>> + Send;
}

/// Creates connections to a real Postgres (or Postgres-compatible) server over TCP.
/// Always attempts TLS first; falls back to plaintext if the server declines.
pub struct TcpConnectionFactory {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: Option<String>,
    pub database: String,
}

/// The stream type: either TLS-wrapped or plain TCP.
#[allow(clippy::large_enum_variant)]
pub enum MaybeTlsStream {
    Tls(tokio_rustls::client::TlsStream<TcpStream>),
    Plain(TcpStream),
}

impl AsyncRead for MaybeTlsStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Tls(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            MaybeTlsStream::Plain(s) => std::pin::Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for MaybeTlsStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        match self.get_mut() {
            MaybeTlsStream::Tls(s) => std::pin::Pin::new(s).poll_write(cx, buf),
            MaybeTlsStream::Plain(s) => std::pin::Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Tls(s) => std::pin::Pin::new(s).poll_flush(cx),
            MaybeTlsStream::Plain(s) => std::pin::Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Tls(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            MaybeTlsStream::Plain(s) => std::pin::Pin::new(s).poll_shutdown(cx),
        }
    }
}

impl ConnectionFactory for TcpConnectionFactory {
    type Stream = MaybeTlsStream;

    async fn connect(&self) -> io::Result<PgConnection<MaybeTlsStream>> {
        let stream = TcpStream::connect((self.host.as_str(), self.port)).await?;
        let mut builder = ConnectionBuilder::new(&self.user).database(&self.database);
        if let Some(pw) = &self.password {
            builder = builder.auth(AuthenticationMode::Password(pw.clone()));
        }

        let host = self.host.clone();

        // Try TLS first. The closure returns MaybeTlsStream so the
        // resulting PgConnection is already the right type.
        match builder
            .connect_with_tls(stream, async |s| {
                let config = tls_config();
                let connector = TlsConnector::from(Arc::new(config));
                let server_name = ServerName::try_from(host)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
                let tls_stream = connector.connect(server_name, s).await?;
                Ok(MaybeTlsStream::Tls(tls_stream))
            })
            .await
        {
            Ok((conn, _startup)) => Ok(conn),
            Err(pg_stream::startup::Error::TlsUnsupported) => {
                // Server declined TLS — reconnect without it.
                tracing::debug!("server declined TLS, falling back to plaintext");
                let stream = TcpStream::connect((self.host.as_str(), self.port)).await?;
                let mut builder = ConnectionBuilder::new(&self.user).database(&self.database);
                if let Some(pw) = &self.password {
                    builder = builder.auth(AuthenticationMode::Password(pw.clone()));
                }
                let (conn, _startup) = builder.connect(stream).await.map_err(io::Error::other)?;
                // Wrap the plain TcpStream into MaybeTlsStream. The read buffer
                // is empty after the startup handshake so no data is lost.
                let (tcp, _buf) = conn.into_parts();
                Ok(PgConnection::new(MaybeTlsStream::Plain(tcp)))
            }
            Err(e) => Err(io::Error::other(e)),
        }
    }
}

/// TLS config that accepts any server certificate.
/// Appropriate for a fuzzer connecting to test infrastructure.
fn tls_config() -> ClientConfig {
    ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoVerify))
        .with_no_client_auth()
}

#[derive(Debug)]
struct NoVerify;

impl ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::aws_lc_rs::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}
