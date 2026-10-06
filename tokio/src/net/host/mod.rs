//! Networking through a link layer the embedding provides, for hosts with no
//! sockets of their own.
//!
//! On `wasm32-unknown-emscripten` inside a JavaScript isolate there is no
//! socket syscall to make: the platform offers streams, promises and
//! callbacks, and (on Cloudflare Workers) a TCP link from a Durable Object
//! to its container. The types in this module are the ordinary
//! [`TcpStream`], [`TcpSocket`], [`UdpSocket`] and [`lookup_host`] API,
//! implemented over two traits supplied by the embedding:
//!
//! * a [`Dialer`], which opens links: a TCP connection to a host and port, a
//!   bound UDP socket, or a name lookup;
//! * a [`Link`], one open connection or socket, polled for bytes or
//!   datagrams with readiness-style `poll_*` methods that register the
//!   caller's [`Waker`](std::task::Waker).
//!
//! [`install`] supplies the process default dialer. A hosted runtime can
//! select its own with `LocalEventLoop::set_network_dialer`.
//!
//! The traits carry `Send + Sync` bounds and return `Send` futures, so the
//! socket types are `Send + Sync` the way the native ones are, and a task
//! that owns one can be spawned with `tokio::spawn`. An implementation that
//! wraps single-threaded platform handles keeps those handles in a registry
//! keyed by integer ids and lets the `Link` object hold only the id, buffers
//! and wakers: no `unsafe impl Send`.
//!
//! Semantics the implementation must honor, because the socket types rely
//! on them:
//!
//! * `poll_*` methods are readiness-style: `Pending` registers the waker and
//!   guarantees a wake when the operation could make progress. A later
//!   registration replaces an earlier one (one reader, one writer at a time,
//!   as with the native types).
//! * `try_*` methods never register a waker and return `WouldBlock`.
//! * Datagram boundaries are preserved. `poll_recv_from` fills the caller's
//!   buffer with one datagram, truncating a larger one, like `recvfrom`.
//! * A UDP link bound with [`Dialer::bind_udp`] is bidirectional on one
//!   port: data arrives from any peer and status messages go back out the
//!   same port, which is what Aeron and Discord voice both require.
//!
//! On the Emscripten target this module *is* `tokio::net`. On other targets
//! it compiles under `--cfg tokio_host_net` as `tokio::net::host`, so the
//! same code is tested on the multi-thread runtime against an in-memory
//! dialer.

use std::cell::RefCell;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use std::time::Duration;

use crate::io::ReadBuf;

mod tcp;
mod udp;
#[cfg(unix)]
mod unix;

pub use tcp::{OwnedReadHalf, OwnedWriteHalf, ReadHalf, ReuniteError, TcpListener, TcpSocket, TcpStream, WriteHalf};
pub use udp::UdpSocket;
#[cfg(unix)]
pub use unix::{UnixDatagram, UnixListener, UnixStream};

/// A future the dialer returns. `Send`, so a connecting task may be spawned.
pub type DialFuture = Pin<Box<dyn Future<Output = io::Result<Arc<dyn Link>>> + Send + 'static>>;
/// A name lookup's future.
pub type ResolveFuture = Pin<Box<dyn Future<Output = io::Result<Vec<SocketAddr>>> + Send + 'static>>;

/// Where a TCP connection goes: an address, or a name the link layer
/// resolves itself (a host that resolves at connect time, as the Workers
/// `connect()` API does, need not round-trip a lookup first).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    /// A resolved address.
    Addr(SocketAddr),
    /// A host name and port.
    Name(String, u16),
}

/// Options a [`TcpSocket`] collects before `connect`.
#[derive(Debug, Clone, Default)]
pub struct TcpOptions {
    /// `TCP_NODELAY`.
    pub nodelay: Option<bool>,
    /// `SO_KEEPALIVE`.
    pub keepalive: Option<bool>,
    /// `SO_SNDBUF`, in bytes.
    pub send_buffer_size: Option<u32>,
    /// `SO_RCVBUF`, in bytes.
    pub recv_buffer_size: Option<u32>,
    /// Keep the write side open after the peer sends EOF.
    pub allow_half_open: bool,
}

/// Options for [`Dialer::bind_udp`].
#[derive(Debug, Clone, Default)]
pub struct UdpOptions {
    /// `SO_RCVBUF`, in bytes. A stream at Aeron rates wants megabytes here.
    pub recv_buffer_size: Option<u32>,
    /// `SO_SNDBUF`, in bytes.
    pub send_buffer_size: Option<u32>,
    /// `SO_BROADCAST`.
    pub broadcast: Option<bool>,
    /// `IP_TTL`.
    pub ttl: Option<u32>,
}

/// A socket option set after the link is open. The link may answer
/// `Unsupported`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SocketOption {
    /// `TCP_NODELAY`.
    Nodelay(bool),
    /// `SO_BROADCAST`.
    Broadcast(bool),
    /// `IP_TTL`.
    Ttl(u32),
    /// `IP_TOS`.
    Tos(u32),
    /// `IP_MULTICAST_LOOP`.
    MulticastLoopV4(bool),
    /// `IP_MULTICAST_TTL`.
    MulticastTtlV4(u32),
    /// `IPV6_MULTICAST_LOOP`.
    MulticastLoopV6(bool),
    /// `SO_LINGER`.
    Linger(Option<Duration>),
}

/// Counters a link keeps. Zero when the link does not track one.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LinkStats {
    /// Datagrams the link layer dropped because the receiver fell behind.
    pub datagrams_dropped: u64,
    /// Datagrams delivered to the socket.
    pub datagrams_received: u64,
    /// Datagrams sent.
    pub datagrams_sent: u64,
    /// Bytes read (TCP) or received (UDP payload).
    pub bytes_in: u64,
    /// Bytes written or sent.
    pub bytes_out: u64,
    /// Pieces the link layer delivered: one per read or message from the
    /// transport. `datagrams_received / chunks_in` is the batch size the
    /// far end achieved, which sets the per-datagram cost on a host where
    /// every piece is one event loop turn.
    pub chunks_in: u64,
}

/// One open connection or socket.
pub trait Link: Send + Sync + 'static {
    // ----- byte stream (TCP) -----

    fn poll_read(&self, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>>;
    fn poll_write(&self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>>;
    fn poll_flush(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>>;
    /// Half-close the write side.
    fn poll_shutdown(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>>;
    /// Ready when `try_read` would not return `WouldBlock`.
    fn poll_read_ready(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>>;
    /// Ready when `try_write` would not return `WouldBlock`.
    fn poll_write_ready(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>>;
    fn try_read(&self, buf: &mut [u8]) -> io::Result<usize>;
    fn try_write(&self, buf: &[u8]) -> io::Result<usize>;

    // ----- datagrams (UDP) -----

    /// One datagram into `buf` (truncated if larger), with its source.
    fn poll_recv_from(&self, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<SocketAddr>>;
    /// One datagram to `target`, or to the connected peer when `None`.
    fn poll_send_to(&self, cx: &mut Context<'_>, buf: &[u8], target: Option<SocketAddr>) -> Poll<io::Result<usize>>;
    fn try_recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)>;
    fn try_send_to(&self, buf: &[u8], target: Option<SocketAddr>) -> io::Result<usize>;
    /// Set the default peer: `send`/`recv` without an address use it, and
    /// datagrams from other peers are filtered.
    fn connect_peer(&self, peer: SocketAddr) -> io::Result<()>;

    // ----- both -----

    fn local_addr(&self) -> io::Result<SocketAddr>;
    fn peer_addr(&self) -> io::Result<SocketAddr>;
    fn set_option(&self, option: SocketOption) -> io::Result<()>;
    /// The last asynchronous error, if the link stores one.
    fn take_error(&self) -> io::Result<Option<io::Error>>;
    fn stats(&self) -> LinkStats;
}

/// Opens links.
pub trait Dialer: Send + Sync + 'static {
    fn connect_tcp(&self, target: Target, options: TcpOptions) -> DialFuture;
    fn bind_udp(&self, local: SocketAddr, options: UdpOptions) -> DialFuture;
    fn resolve(&self, host: String, port: u16) -> ResolveFuture;
}

impl std::fmt::Debug for dyn Dialer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Dialer")
    }
}

static DIALER: OnceLock<Arc<dyn Dialer>> = OnceLock::new();

thread_local! {
    static CURRENT_DIALER: RefCell<Option<Arc<dyn Dialer>>> = const { RefCell::new(None) };
}

cfg_host_loop! {
    pub(crate) struct DialerGuard {
        previous: Option<Arc<dyn Dialer>>,
        _not_send: std::marker::PhantomData<std::rc::Rc<()>>,
    }

    pub(crate) fn enter_dialer(dialer: Option<Arc<dyn Dialer>>) -> DialerGuard {
        DialerGuard {
            previous: CURRENT_DIALER.with(|current| current.replace(dialer)),
            _not_send: std::marker::PhantomData,
        }
    }

    impl Drop for DialerGuard {
        fn drop(&mut self) {
            CURRENT_DIALER.with(|current| current.replace(self.previous.take()));
        }
    }
}

/// Installs the process's default dialer. A `LocalEventLoop` may select its
/// own with `set_network_dialer`. The first installation wins, and the
/// dialer is handed back if one is installed already.
pub fn install(dialer: Arc<dyn Dialer>) -> Result<(), Arc<dyn Dialer>> {
    DIALER.set(dialer)
}

pub(crate) fn dialer() -> io::Result<Arc<dyn Dialer>> {
    CURRENT_DIALER
        .with(|current| current.borrow().clone())
        .or_else(|| DIALER.get().cloned())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                "no network dialer is installed; call LocalEventLoop::set_network_dialer or tokio::net::host::install first",
            )
        })
}

/// Resolves `host` to addresses carrying `port` through the installed
/// dialer. An address literal resolves without a round trip.
pub fn resolve(host: String, port: u16) -> ResolveFuture {
    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        return Box::pin(async move { Ok(vec![SocketAddr::new(ip, port)]) });
    }
    match dialer() {
        Ok(dialer) => dialer.resolve(host, port),
        Err(e) => Box::pin(async move { Err(e) }),
    }
}

/// The error every unsupported operation returns.
pub(crate) fn unsupported(what: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::Unsupported, what)
}
