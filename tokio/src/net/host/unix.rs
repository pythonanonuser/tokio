//! Unix domain sockets do not exist on a link layer. The types are kept so
//! crates that name them under `cfg(unix)` (hyper-util's connector, for one)
//! compile for the host target; every operation reports `Unsupported`.

use super::unsupported;
use crate::io::{AsyncRead, AsyncWrite, ReadBuf};

use std::io;
use std::os::unix::net::SocketAddr;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

const WHAT: &str = "unix domain sockets are not available on a link layer";

/// A Unix stream socket. Not available on a link layer; see the module docs.
#[derive(Debug)]
pub struct UnixStream {
    _private: (),
}

impl UnixStream {
    pub async fn connect<P: AsRef<Path>>(_path: P) -> io::Result<UnixStream> {
        Err(unsupported(WHAT))
    }

    pub fn pair() -> io::Result<(UnixStream, UnixStream)> {
        Err(unsupported(WHAT))
    }

    pub fn from_std(_stream: std::os::unix::net::UnixStream) -> io::Result<UnixStream> {
        Err(unsupported(WHAT))
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        Err(unsupported(WHAT))
    }

    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        Err(unsupported(WHAT))
    }

    pub fn take_error(&self) -> io::Result<Option<io::Error>> {
        Err(unsupported(WHAT))
    }
}

impl AsyncRead for UnixStream {
    fn poll_read(self: Pin<&mut Self>, _cx: &mut Context<'_>, _buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Err(unsupported(WHAT)))
    }
}

impl AsyncWrite for UnixStream {
    fn poll_write(self: Pin<&mut Self>, _cx: &mut Context<'_>, _buf: &[u8]) -> Poll<io::Result<usize>> {
        Poll::Ready(Err(unsupported(WHAT)))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Err(unsupported(WHAT)))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Err(unsupported(WHAT)))
    }
}

/// A Unix listener. Not available on a link layer.
#[derive(Debug)]
pub struct UnixListener {
    _private: (),
}

impl UnixListener {
    pub fn bind<P: AsRef<Path>>(_path: P) -> io::Result<UnixListener> {
        Err(unsupported(WHAT))
    }

    pub async fn accept(&self) -> io::Result<(UnixStream, SocketAddr)> {
        Err(unsupported(WHAT))
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        Err(unsupported(WHAT))
    }
}

/// A Unix datagram socket. Not available on a link layer.
#[derive(Debug)]
pub struct UnixDatagram {
    _private: (),
}

impl UnixDatagram {
    pub fn bind<P: AsRef<Path>>(_path: P) -> io::Result<UnixDatagram> {
        Err(unsupported(WHAT))
    }

    pub fn unbound() -> io::Result<UnixDatagram> {
        Err(unsupported(WHAT))
    }

    pub fn pair() -> io::Result<(UnixDatagram, UnixDatagram)> {
        Err(unsupported(WHAT))
    }
}
