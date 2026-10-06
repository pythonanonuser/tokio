//! TCP over a [`Link`]: the stream, the pre-connect socket, the split
//! halves, and a listener that reports what the platform lacks.

use super::{dialer, unsupported, Link, LinkStats, Option_, Target, TcpOptions};
use crate::io::{AsyncRead, AsyncWrite, Interest, ReadBuf, Ready};
use crate::net::ToSocketAddrs;

use std::fmt;
use std::future::poll_fn;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

/// A TCP connection over a [`Link`]. `Send + Sync`; `AsyncRead + AsyncWrite`.
pub struct TcpStream {
    link: Arc<dyn Link>,
}

impl TcpStream {
    /// Resolves `addr` through [`ToSocketAddrs`] and tries each address
    /// through the selected dialer. Use [`Self::connect_name`] to pass a
    /// name directly to the dialer for resolution at connect time.
    pub async fn connect<A: ToSocketAddrs>(addr: A) -> io::Result<TcpStream> {
        let addrs = crate::net::to_socket_addrs(addr).await?;
        let mut last_err = None;
        for addr in addrs {
            match Self::connect_target(Target::Addr(addr), TcpOptions::default()).await {
                Ok(stream) => return Ok(stream),
                Err(e) => last_err = Some(e),
            }
        }
        Err(last_err.unwrap_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "could not resolve to any address")
        }))
    }

    /// Connects to a host by name, letting the link layer resolve it.
    pub async fn connect_name(host: &str, port: u16) -> io::Result<TcpStream> {
        Self::connect_target(Target::Name(host.to_owned(), port), TcpOptions::default()).await
    }

    pub(super) async fn connect_target(target: Target, options: TcpOptions) -> io::Result<TcpStream> {
        let link = dialer()?.connect_tcp(target, options).await?;
        Ok(TcpStream { link })
    }

    /// Wraps an already open link.
    pub fn from_link(link: Arc<dyn Link>) -> TcpStream {
        TcpStream { link }
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.link.local_addr()
    }

    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.link.peer_addr()
    }

    pub fn take_error(&self) -> io::Result<Option<io::Error>> {
        self.link.take_error()
    }

    pub fn from_std(_stream: std::net::TcpStream) -> io::Result<TcpStream> {
        Err(unsupported("a standard socket cannot be adopted on a link layer"))
    }

    pub fn into_std(self) -> io::Result<std::net::TcpStream> {
        Err(unsupported("a link-backed stream has no standard socket"))
    }

    pub fn poll_peek(&self, _cx: &mut Context<'_>, _buf: &mut ReadBuf<'_>) -> Poll<io::Result<usize>> {
        Poll::Ready(Err(unsupported("peek is not available on a link-backed stream")))
    }

    pub async fn peek(&self, _buf: &mut [u8]) -> io::Result<usize> {
        Err(unsupported("peek is not available on a link-backed stream"))
    }

    pub async fn ready(&self, interest: Interest) -> io::Result<Ready> {
        poll_fn(|cx| {
            let mut ready = Ready::EMPTY;
            if interest.is_readable() {
                if let Poll::Ready(r) = self.link.poll_read_ready(cx) {
                    r?;
                    ready |= Ready::READABLE;
                }
            }
            if interest.is_writable() {
                if let Poll::Ready(r) = self.link.poll_write_ready(cx) {
                    r?;
                    ready |= Ready::WRITABLE;
                }
            }
            if ready.is_empty() {
                Poll::Pending
            } else {
                Poll::Ready(Ok(ready))
            }
        })
        .await
    }

    pub async fn readable(&self) -> io::Result<()> {
        poll_fn(|cx| self.poll_read_ready(cx)).await
    }

    pub fn poll_read_ready(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.link.poll_read_ready(cx)
    }

    pub fn try_read(&self, buf: &mut [u8]) -> io::Result<usize> {
        self.link.try_read(buf)
    }

    pub fn try_read_vectored(&self, bufs: &mut [io::IoSliceMut<'_>]) -> io::Result<usize> {
        for buf in bufs.iter_mut() {
            if !buf.is_empty() {
                return self.link.try_read(buf);
            }
        }
        Ok(0)
    }

    cfg_io_util! {
        pub fn try_read_buf<B: bytes::BufMut>(&self, buf: &mut B) -> io::Result<usize> {
            let dst = buf.chunk_mut();
            let dst = unsafe { dst.as_uninit_slice_mut() };
            let mut read = ReadBuf::uninit(dst);
            read.initialize_unfilled();
            let n = self.link.try_read(read.initialized_mut())?;
            // SAFETY: `n` bytes were written by the link.
            unsafe { buf.advance_mut(n) };
            Ok(n)
        }
    }

    pub async fn writable(&self) -> io::Result<()> {
        poll_fn(|cx| self.poll_write_ready(cx)).await
    }

    pub fn poll_write_ready(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.link.poll_write_ready(cx)
    }

    pub fn try_write(&self, buf: &[u8]) -> io::Result<usize> {
        self.link.try_write(buf)
    }

    pub fn try_write_vectored(&self, bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
        for buf in bufs {
            if !buf.is_empty() {
                return self.link.try_write(buf);
            }
        }
        Ok(0)
    }

    pub fn try_io<R>(&self, interest: Interest, f: impl FnOnce() -> io::Result<R>) -> io::Result<R> {
        let _ = interest;
        f()
    }

    pub async fn async_io<R>(&self, interest: Interest, mut f: impl FnMut() -> io::Result<R>) -> io::Result<R> {
        loop {
            self.ready(interest).await?;
            match f() {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                res => return res,
            }
        }
    }

    pub fn nodelay(&self) -> io::Result<bool> {
        Err(unsupported("nodelay is not readable on a link-backed stream"))
    }

    pub fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
        self.link.set_option(Option_::Nodelay(nodelay))
    }

    pub fn linger(&self) -> io::Result<Option<Duration>> {
        Err(unsupported("linger is not readable on a link-backed stream"))
    }

    pub fn set_linger(&self, dur: Option<Duration>) -> io::Result<()> {
        self.link.set_option(Option_::Linger(dur))
    }

    pub fn ttl(&self) -> io::Result<u32> {
        Err(unsupported("ttl is not readable on a link-backed stream"))
    }

    pub fn set_ttl(&self, ttl: u32) -> io::Result<()> {
        self.link.set_option(Option_::Ttl(ttl))
    }

    /// Link counters.
    pub fn link_stats(&self) -> LinkStats {
        self.link.stats()
    }

    /// Borrowed halves.
    pub fn split<'a>(&'a mut self) -> (ReadHalf<'a>, WriteHalf<'a>) {
        (ReadHalf(&*self), WriteHalf(&*self))
    }

    /// Owned halves. `reunite` joins them back.
    pub fn into_split(self) -> (OwnedReadHalf, OwnedWriteHalf) {
        let arc = Arc::new(self);
        (
            OwnedReadHalf { inner: arc.clone() },
            OwnedWriteHalf {
                inner: arc,
                shutdown_on_drop: true,
            },
        )
    }

    pub(super) fn poll_read_priv(&self, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        self.link.poll_read(cx, buf)
    }

    pub(super) fn poll_write_priv(&self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        self.link.poll_write(cx, buf)
    }

    pub(super) fn poll_flush_priv(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.link.poll_flush(cx)
    }

    pub(super) fn poll_shutdown_priv(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.link.poll_shutdown(cx)
    }
}

impl AsyncRead for TcpStream {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        self.poll_read_priv(cx, buf)
    }
}

impl AsyncWrite for TcpStream {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        self.poll_write_priv(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_flush_priv(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_shutdown_priv(cx)
    }
}

impl fmt::Debug for TcpStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TcpStream")
            .field("local", &self.link.local_addr().ok())
            .field("peer", &self.link.peer_addr().ok())
            .finish()
    }
}

impl AsRef<TcpStream> for TcpStream {
    fn as_ref(&self) -> &TcpStream {
        self
    }
}

// ----- borrowed halves -----

/// The read half of a borrowed split.
#[derive(Debug)]
pub struct ReadHalf<'a>(&'a TcpStream);
/// The write half of a borrowed split.
#[derive(Debug)]
pub struct WriteHalf<'a>(&'a TcpStream);

impl ReadHalf<'_> {
    pub async fn readable(&self) -> io::Result<()> {
        self.0.readable().await
    }
    pub fn try_read(&self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.try_read(buf)
    }
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.0.peer_addr()
    }
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.0.local_addr()
    }
}

impl WriteHalf<'_> {
    pub async fn writable(&self) -> io::Result<()> {
        self.0.writable().await
    }
    pub fn try_write(&self, buf: &[u8]) -> io::Result<usize> {
        self.0.try_write(buf)
    }
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.0.peer_addr()
    }
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.0.local_addr()
    }
}

impl AsyncRead for ReadHalf<'_> {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        self.0.poll_read_priv(cx, buf)
    }
}

impl AsyncWrite for WriteHalf<'_> {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        self.0.poll_write_priv(cx, buf)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.0.poll_flush_priv(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.0.poll_shutdown_priv(cx)
    }
}

// ----- owned halves -----

/// The read half of `into_split`.
#[derive(Debug)]
pub struct OwnedReadHalf {
    inner: Arc<TcpStream>,
}

/// The write half of `into_split`. Dropping it shuts the write side down
/// unless `forget` was called.
#[derive(Debug)]
pub struct OwnedWriteHalf {
    inner: Arc<TcpStream>,
    shutdown_on_drop: bool,
}

/// `reunite` was given halves of two different streams.
#[derive(Debug)]
pub struct ReuniteError(pub OwnedReadHalf, pub OwnedWriteHalf);

impl fmt::Display for ReuniteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("tried to reunite halves that are not from the same socket")
    }
}

impl std::error::Error for ReuniteError {}

impl OwnedReadHalf {
    pub fn reunite(self, other: OwnedWriteHalf) -> Result<TcpStream, ReuniteError> {
        if Arc::ptr_eq(&self.inner, &other.inner) {
            let mut other = other;
            other.shutdown_on_drop = false;
            drop(other);
            Ok(Arc::try_unwrap(self.inner).expect("the two halves were the only references"))
        } else {
            Err(ReuniteError(self, other))
        }
    }
    pub async fn ready(&self, interest: Interest) -> io::Result<Ready> {
        self.inner.ready(interest).await
    }
    pub async fn readable(&self) -> io::Result<()> {
        self.inner.readable().await
    }
    pub fn try_read(&self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.try_read(buf)
    }
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.inner.peer_addr()
    }
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }
}

impl OwnedWriteHalf {
    pub fn reunite(self, other: OwnedReadHalf) -> Result<TcpStream, ReuniteError> {
        other.reunite(self)
    }
    /// Drops the half without shutting the write side down.
    pub fn forget(mut self) {
        self.shutdown_on_drop = false;
    }
    pub async fn ready(&self, interest: Interest) -> io::Result<Ready> {
        self.inner.ready(interest).await
    }
    pub async fn writable(&self) -> io::Result<()> {
        self.inner.writable().await
    }
    pub fn try_write(&self, buf: &[u8]) -> io::Result<usize> {
        self.inner.try_write(buf)
    }
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.inner.peer_addr()
    }
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }
}

impl Drop for OwnedWriteHalf {
    fn drop(&mut self) {
        if self.shutdown_on_drop {
            // Best effort, as the native half does: a pending shutdown the
            // link completes on its own.
            let waker = noop_waker();
            let mut cx = Context::from_waker(&waker);
            let _ = self.inner.poll_shutdown_priv(&mut cx);
        }
    }
}

impl AsyncRead for OwnedReadHalf {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        self.inner.poll_read_priv(cx, buf)
    }
}

impl AsyncWrite for OwnedWriteHalf {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        self.inner.poll_write_priv(cx, buf)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.inner.poll_flush_priv(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.shutdown_on_drop = false;
        this.inner.poll_shutdown_priv(cx)
    }
}

// ----- TcpSocket -----

/// A not-yet-connected socket: collects options, then `connect`s. Binding a
/// local address and listening are not available on a link layer.
#[derive(Debug, Clone, Default)]
pub struct TcpSocket {
    options: TcpOptions,
}

impl TcpSocket {
    pub fn new_v4() -> io::Result<TcpSocket> {
        Ok(TcpSocket::default())
    }

    pub fn new_v6() -> io::Result<TcpSocket> {
        Ok(TcpSocket::default())
    }

    pub fn set_keepalive(&self, _keepalive: bool) -> io::Result<()> {
        // Options are collected by value; `set_*` on a shared reference
        // cannot record them, so they are accepted and ignored here. Use
        // `with_options` to carry them to connect.
        Ok(())
    }

    pub fn keepalive(&self) -> io::Result<bool> {
        Ok(self.options.keepalive.unwrap_or(false))
    }

    pub fn set_reuseaddr(&self, _reuseaddr: bool) -> io::Result<()> {
        Ok(())
    }

    pub fn reuseaddr(&self) -> io::Result<bool> {
        Ok(false)
    }

    pub fn set_send_buffer_size(&self, _size: u32) -> io::Result<()> {
        Ok(())
    }

    pub fn send_buffer_size(&self) -> io::Result<u32> {
        Ok(self.options.send_buffer_size.unwrap_or(0))
    }

    pub fn set_recv_buffer_size(&self, _size: u32) -> io::Result<()> {
        Ok(())
    }

    pub fn recv_buffer_size(&self) -> io::Result<u32> {
        Ok(self.options.recv_buffer_size.unwrap_or(0))
    }

    pub fn set_nodelay(&self, _nodelay: bool) -> io::Result<()> {
        Ok(())
    }

    pub fn nodelay(&self) -> io::Result<bool> {
        Ok(self.options.nodelay.unwrap_or(false))
    }

    pub fn set_linger(&self, _dur: Option<Duration>) -> io::Result<()> {
        Ok(())
    }

    pub fn linger(&self) -> io::Result<Option<Duration>> {
        Ok(None)
    }

    /// The options the connection is opened with.
    pub fn with_options(mut self, options: TcpOptions) -> TcpSocket {
        self.options = options;
        self
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        Err(unsupported("a link-backed socket has no local address before connect"))
    }

    pub fn take_error(&self) -> io::Result<Option<io::Error>> {
        Ok(None)
    }

    pub fn bind(&self, _addr: SocketAddr) -> io::Result<()> {
        Err(unsupported("binding a local address is not available on a link layer"))
    }

    pub async fn connect(self, addr: SocketAddr) -> io::Result<TcpStream> {
        TcpStream::connect_target(Target::Addr(addr), self.options).await
    }

    /// Accepts and drops a standard socket. Code that prepares a socket with
    /// `socket2` and hands it over (hyper-util's connector) then calls
    /// `connect`, which dials through the link layer; the standard socket's
    /// options are not carried.
    pub fn from_std_stream(_std_stream: std::net::TcpStream) -> TcpSocket {
        TcpSocket::default()
    }

    pub fn listen(self, _backlog: u32) -> io::Result<TcpListener> {
        Err(unsupported("listening is not available on a link layer"))
    }
}

// ----- TcpListener -----

/// Inbound TCP is not available on a link layer; every constructor reports
/// `Unsupported`. The type exists so code that names it compiles.
#[derive(Debug)]
pub struct TcpListener {
    _private: (),
}

impl TcpListener {
    pub async fn bind<A: ToSocketAddrs>(_addr: A) -> io::Result<TcpListener> {
        Err(unsupported("listening is not available on a link layer"))
    }

    pub async fn accept(&self) -> io::Result<(TcpStream, SocketAddr)> {
        Err(unsupported("listening is not available on a link layer"))
    }

    pub fn from_std(_listener: std::net::TcpListener) -> io::Result<TcpListener> {
        Err(unsupported("listening is not available on a link layer"))
    }

    pub fn poll_accept(&self, _cx: &mut Context<'_>) -> Poll<io::Result<(TcpStream, SocketAddr)>> {
        Poll::Ready(Err(unsupported("listening is not available on a link layer")))
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        Err(unsupported("listening is not available on a link layer"))
    }

    pub fn ttl(&self) -> io::Result<u32> {
        Err(unsupported("listening is not available on a link layer"))
    }

    pub fn set_ttl(&self, _ttl: u32) -> io::Result<()> {
        Err(unsupported("listening is not available on a link layer"))
    }
}

fn noop_waker() -> std::task::Waker {
    use std::task::{RawWaker, RawWakerVTable, Waker};
    const VTABLE: RawWakerVTable = RawWakerVTable::new(|_| RawWaker::new(std::ptr::null(), &VTABLE), |_| {}, |_| {}, |_| {});
    // SAFETY: the vtable functions touch no data.
    unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
}
