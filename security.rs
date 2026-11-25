//! TLS 1.3 with AES-256-GCM and X25519 and no resumption, for every Rust server and client, from keys.sh's files.

use std::{
    io,
    net::SocketAddr,
    path::Path,
    sync::{Arc, OnceLock},
};

use rustls::{
    client::Resumption,
    crypto::{aws_lc_rs, CryptoProvider},
    pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer, ServerName},
    server::NoServerSessionStorage,
    ClientConfig, RootCertStore, ServerConfig,
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpListener, TcpStream},
    sync::mpsc,
};
use tokio_rustls::{server::TlsStream, TlsAcceptor, TlsConnector};

use crate::arguments::{self, CommonArgs, Interface, Security};

/// Exits unless the framework speaks the requested security; only UCall speaks Noise.
pub fn require_tls_or_none(common: &CommonArgs, framework: &str) {
    if matches!(common.security, Security::NoiseXx | Security::NoiseIk) {
        arguments::exit(&format!("{framework} has no Noise; use --security none or tls"));
    }
}

fn provider() -> Arc<CryptoProvider> {
    Arc::new(CryptoProvider {
        cipher_suites: vec![aws_lc_rs::cipher_suite::TLS13_AES_256_GCM_SHA384],
        kx_groups: vec![aws_lc_rs::kx_group::X25519],
        ..aws_lc_rs::default_provider()
    })
}

/// The server's TLS policy offering `alpn`, or `None` for plaintext.
pub fn tls_server_config(common: &CommonArgs, alpn: &[&[u8]]) -> io::Result<Option<Arc<ServerConfig>>> {
    if common.security != Security::Tls {
        return Ok(None);
    }
    let directory = common.data_dir.join("tls");
    let mut config = ServerConfig::builder_with_provider(provider())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(io::Error::other)?
        .with_no_client_auth()
        .with_single_cert(
            certificates(&directory.join("server.pem"))?,
            private_key(&directory.join("server.key"))?,
        )
        .map_err(io::Error::other)?;
    config.session_storage = Arc::new(NoServerSessionStorage {});
    config.send_tls13_tickets = 0;
    config.alpn_protocols = alpn.iter().map(|protocol| protocol.to_vec()).collect();
    Ok(Some(Arc::new(config)))
}

/// The client's TLS policy offering `alpn`, trusting keys.sh's CA when `--security tls` asks for it, which callers
/// check before using the policy.
pub fn tls_client_config(common: &CommonArgs, alpn: &[&[u8]]) -> io::Result<Arc<ClientConfig>> {
    let mut roots = RootCertStore::empty();
    if common.security == Security::Tls {
        for certificate in certificates(&common.data_dir.join("tls").join("ca.pem"))? {
            roots.add(certificate).map_err(io::Error::other)?;
        }
    }
    let mut config = ClientConfig::builder_with_provider(provider())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(io::Error::other)?
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.resumption = Resumption::disabled();
    config.alpn_protocols = alpn.iter().map(|protocol| protocol.to_vec()).collect();
    Ok(Arc::new(config))
}

/// The connector a client uses: TLS under `--security tls`, otherwise none.
pub fn tls_connector(common: &CommonArgs, alpn: &[&[u8]]) -> io::Result<Option<TlsConnector>> {
    match common.security {
        Security::Tls => Ok(Some(TlsConnector::from(tls_client_config(common, alpn)?))),
        _ => Ok(None),
    }
}

fn certificates(path: &Path) -> io::Result<Vec<CertificateDer<'static>>> {
    let parsed = CertificateDer::pem_file_iter(path).map_err(|error| pem_error(path, error))?;
    parsed.collect::<Result<_, _>>().map_err(|error| pem_error(path, error))
}

fn private_key(path: &Path) -> io::Result<PrivateKeyDer<'static>> {
    PrivateKeyDer::from_pem_file(path).map_err(|error| pem_error(path, error))
}

fn pem_error(path: &Path, error: rustls::pki_types::pem::Error) -> io::Error {
    io::Error::other(format!("{}: {error}; run keys.sh first", path.display()))
}

/// A connection the clients drive, plaintext or TLS.
pub trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}
pub type Stream = Box<dyn Io>;

static NEGOTIATED_SUITE: OnceLock<String> = OnceLock::new();

/// The cipher suite the first TLS handshake settled on, for the reports.
pub fn negotiated_suite() -> Option<String> {
    NEGOTIATED_SUITE.get().cloned()
}

/// Connects to `target` from the interface, completing a TLS handshake when `tls` is set.
pub async fn connect(
    interface: &Interface,
    index: usize,
    target: SocketAddr,
    tls: Option<&TlsConnector>,
) -> io::Result<Stream> {
    let stream = arguments::connect(interface, index, target).await?;
    let Some(tls) = tls else {
        return Ok(Box::new(stream));
    };
    let stream = tls.connect(ServerName::IpAddress(target.ip().into()), stream).await?;
    if let Some(suite) = stream.get_ref().1.negotiated_cipher_suite() {
        NEGOTIATED_SUITE.get_or_init(|| format!("{:?}", suite.suite()));
    }
    Ok(Box::new(stream))
}

/// A listener handing out connections whose TLS handshakes finished, each handshake on its own task.
pub struct TlsListener {
    accepted: mpsc::Receiver<(TlsStream<TcpStream>, SocketAddr)>,
    /// The address the listener bound.
    pub local: SocketAddr,
}

impl TlsListener {
    pub fn new(listener: TcpListener, config: Arc<ServerConfig>) -> io::Result<Self> {
        let local = listener.local_addr()?;
        let acceptor = TlsAcceptor::from(config);
        let (sender, accepted) = mpsc::channel(1024);
        tokio::spawn(async move {
            loop {
                let Ok((stream, peer)) = listener.accept().await else {
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    continue;
                };
                let (acceptor, sender) = (acceptor.clone(), sender.clone());
                tokio::spawn(async move {
                    if let Ok(stream) = acceptor.accept(stream).await {
                        let _ = sender.send((stream, peer)).await;
                    }
                });
            }
        });
        Ok(Self { accepted, local })
    }

    pub async fn next(&mut self) -> (TlsStream<TcpStream>, SocketAddr) {
        self.accepted
            .recv()
            .await
            .expect("the accepting task outlives the listener")
    }

    /// The handshaken connections as a stream, the form tonic serves.
    pub fn into_stream(self) -> impl futures::Stream<Item = io::Result<TlsStream<TcpStream>>> {
        futures::stream::unfold(self, |mut listener| async move {
            Some((Ok(listener.next().await.0), listener))
        })
    }
}

#[cfg(feature = "axum")]
impl axum::serve::Listener for TlsListener {
    type Io = TlsStream<TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        self.next().await
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        Ok(self.local)
    }
}

/// A lazily connecting tonic channel to `address` through `security::connect`, so gRPC binds the same device and
/// source addresses, and speaks the same TLS, as every other Rust client.
#[cfg(feature = "tonic")]
pub fn tonic_channel(
    interface: Interface,
    index: usize,
    address: SocketAddr,
    tls: Option<TlsConnector>,
) -> tonic::transport::Channel {
    let endpoint =
        tonic::transport::Endpoint::from_shared(format!("http://{address}")).expect("an address forms a URI");
    endpoint.connect_with_connector_lazy(tower::service_fn(move |_: tonic::transport::Uri| {
        let (interface, tls) = (interface.clone(), tls.clone());
        async move {
            connect(&interface, index, address, tls.as_ref())
                .await
                .map(hyper_util::rt::TokioIo::new)
        }
    }))
}
