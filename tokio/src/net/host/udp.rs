//! A UDP socket over a [`Link`].

use super::{dialer, unsupported, Link, LinkStats, Option_, UdpOptions};
use crate::io::{Interest, ReadBuf, Ready};
use crate::net::ToSocketAddrs;

use std::fmt;
use std::future::poll_fn;
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

/// A UDP socket whose datagrams travel over a [`Link`] the installed
/// [`Dialer`](super::Dialer) opened. The API follows the native
/// `tokio::net::UdpSocket`; operations the link layer cannot provide
/// (`peek*`, device binding, multicast groups) return `Unsupported`.
pub struct UdpSocket {
    link: Arc<dyn Link>,
    peer: Mutex<Option<SocketAddr>>,
}

impl UdpSocket {
    /// Binds a socket through the installed dialer. The first address `addr`
    /// resolves to is used.
    pub async fn bind<A: ToSocketAddrs>(addr: A) -> io::Result<UdpSocket> {
        let addrs = crate::net::to_socket_addrs(addr).await?;
        let mut last_err = None;
        for local in addrs {
            match Self::bind_addr(local, UdpOptions::default()).await {
                Ok(socket) => return Ok(socket),
                Err(e) => last_err = Some(e),
            }
        }
        Err(last_err.unwrap_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "could not resolve to any address")
        }))
    }

    /// Binds with explicit link options (receive buffer size and the like),
    /// for streams that arrive faster than the default buffers allow.
    pub async fn bind_with(addr: SocketAddr, options: UdpOptions) -> io::Result<UdpSocket> {
        Self::bind_addr(addr, options).await
    }

    async fn bind_addr(local: SocketAddr, options: UdpOptions) -> io::Result<UdpSocket> {
        let link = dialer()?.bind_udp(local, options).await?;
        Ok(UdpSocket {
            link,
            peer: Mutex::new(None),
        })
    }

    /// Wraps an already open link.
    pub fn from_link(link: Arc<dyn Link>) -> UdpSocket {
        UdpSocket {
            link,
            peer: Mutex::new(None),
        }
    }

    pub fn from_std(_socket: std::net::UdpSocket) -> io::Result<UdpSocket> {
        Err(unsupported("a standard socket cannot be adopted on a link layer"))
    }

    pub fn into_std(self) -> io::Result<std::net::UdpSocket> {
        Err(unsupported("a link-backed socket has no standard socket"))
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.link.local_addr()
    }

    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.peer
            .lock()
            .unwrap()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "socket is not connected"))
    }

    /// Sets the default destination and filters incoming datagrams to it.
    pub async fn connect<A: ToSocketAddrs>(&self, addr: A) -> io::Result<()> {
        let mut addrs = crate::net::to_socket_addrs(addr).await?;
        let peer = addrs
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "could not resolve to any address"))?;
        self.link.connect_peer(peer)?;
        *self.peer.lock().unwrap() = Some(peer);
        Ok(())
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

    pub async fn writable(&self) -> io::Result<()> {
        poll_fn(|cx| self.poll_send_ready(cx)).await
    }

    pub fn poll_send_ready(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.link.poll_write_ready(cx)
    }

    pub async fn readable(&self) -> io::Result<()> {
        poll_fn(|cx| self.poll_recv_ready(cx)).await
    }

    pub fn poll_recv_ready(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.link.poll_read_ready(cx)
    }

    pub async fn send(&self, buf: &[u8]) -> io::Result<usize> {
        poll_fn(|cx| self.poll_send(cx, buf)).await
    }

    pub fn poll_send(&self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        self.link.poll_send_to(cx, buf, None)
    }

    pub fn try_send(&self, buf: &[u8]) -> io::Result<usize> {
        self.link.try_send_to(buf, None)
    }

    pub async fn recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        let mut read = ReadBuf::new(buf);
        poll_fn(|cx| self.poll_recv(cx, &mut read)).await?;
        Ok(read.filled().len())
    }

    pub fn poll_recv(&self, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        self.link.poll_recv_from(cx, buf).map_ok(|_| ())
    }

    pub fn try_recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        self.link.try_recv_from(buf).map(|(n, _)| n)
    }

    pub async fn send_to<A: ToSocketAddrs>(&self, buf: &[u8], target: A) -> io::Result<usize> {
        let mut addrs = crate::net::to_socket_addrs(target).await?;
        let target = addrs
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "no addresses to send data to"))?;
        poll_fn(|cx| self.poll_send_to(cx, buf, target)).await
    }

    pub fn poll_send_to(&self, cx: &mut Context<'_>, buf: &[u8], target: SocketAddr) -> Poll<io::Result<usize>> {
        self.link.poll_send_to(cx, buf, Some(target))
    }

    pub fn try_send_to(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize> {
        self.link.try_send_to(buf, Some(target))
    }

    pub async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        let mut read = ReadBuf::new(buf);
        let addr = poll_fn(|cx| self.poll_recv_from(cx, &mut read)).await?;
        Ok((read.filled().len(), addr))
    }

    pub fn poll_recv_from(&self, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<SocketAddr>> {
        self.link.poll_recv_from(cx, buf)
    }

    pub fn try_recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.link.try_recv_from(buf)
    }

    cfg_io_util! {
        pub fn try_recv_buf<B: bytes::BufMut>(&self, buf: &mut B) -> io::Result<usize> {
            self.try_recv_buf_from(buf).map(|(n, _)| n)
        }

        pub async fn recv_buf<B: bytes::BufMut>(&self, buf: &mut B) -> io::Result<usize> {
            self.recv_buf_from(buf).await.map(|(n, _)| n)
        }

        pub fn try_recv_buf_from<B: bytes::BufMut>(&self, buf: &mut B) -> io::Result<(usize, SocketAddr)> {
            let dst = buf.chunk_mut();
            let dst = unsafe { dst.as_uninit_slice_mut() };
            let mut read = ReadBuf::uninit(dst);
            let (n, addr) = {
                // try_recv_from needs an initialized slice; zero the window first.
                read.initialize_unfilled();
                let slice = read.initialized_mut();
                self.link.try_recv_from(slice)?
            };
            // SAFETY: `n` bytes were written by the link.
            unsafe { buf.advance_mut(n) };
            Ok((n, addr))
        }

        pub async fn recv_buf_from<B: bytes::BufMut>(&self, buf: &mut B) -> io::Result<(usize, SocketAddr)> {
            loop {
                self.readable().await?;
                match self.try_recv_buf_from(buf) {
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                    res => return res,
                }
            }
        }
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

    pub async fn peek(&self, _buf: &mut [u8]) -> io::Result<usize> {
        Err(unsupported("peek is not available on a link-backed socket"))
    }

    pub async fn peek_from(&self, _buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        Err(unsupported("peek_from is not available on a link-backed socket"))
    }

    pub async fn peek_sender(&self) -> io::Result<SocketAddr> {
        Err(unsupported("peek_sender is not available on a link-backed socket"))
    }

    pub fn broadcast(&self) -> io::Result<bool> {
        Err(unsupported("broadcast is not readable on a link-backed socket"))
    }

    pub fn set_broadcast(&self, on: bool) -> io::Result<()> {
        self.link.set_option(Option_::Broadcast(on))
    }

    pub fn multicast_loop_v4(&self) -> io::Result<bool> {
        Err(unsupported("multicast options are not readable on a link-backed socket"))
    }

    pub fn set_multicast_loop_v4(&self, on: bool) -> io::Result<()> {
        self.link.set_option(Option_::MulticastLoopV4(on))
    }

    pub fn multicast_ttl_v4(&self) -> io::Result<u32> {
        Err(unsupported("multicast options are not readable on a link-backed socket"))
    }

    pub fn set_multicast_ttl_v4(&self, ttl: u32) -> io::Result<()> {
        self.link.set_option(Option_::MulticastTtlV4(ttl))
    }

    pub fn multicast_loop_v6(&self) -> io::Result<bool> {
        Err(unsupported("multicast options are not readable on a link-backed socket"))
    }

    pub fn set_multicast_loop_v6(&self, on: bool) -> io::Result<()> {
        self.link.set_option(Option_::MulticastLoopV6(on))
    }

    pub fn ttl(&self) -> io::Result<u32> {
        Err(unsupported("ttl is not readable on a link-backed socket"))
    }

    pub fn set_ttl(&self, ttl: u32) -> io::Result<()> {
        self.link.set_option(Option_::Ttl(ttl))
    }

    pub fn tos(&self) -> io::Result<u32> {
        Err(unsupported("tos is not readable on a link-backed socket"))
    }

    pub fn set_tos(&self, tos: u32) -> io::Result<()> {
        self.link.set_option(Option_::Tos(tos))
    }

    pub fn join_multicast_v4(&self, _multiaddr: Ipv4Addr, _interface: Ipv4Addr) -> io::Result<()> {
        Err(unsupported("multicast groups are not available on a link-backed socket"))
    }

    pub fn join_multicast_v6(&self, _multiaddr: &Ipv6Addr, _interface: u32) -> io::Result<()> {
        Err(unsupported("multicast groups are not available on a link-backed socket"))
    }

    pub fn leave_multicast_v4(&self, _multiaddr: Ipv4Addr, _interface: Ipv4Addr) -> io::Result<()> {
        Err(unsupported("multicast groups are not available on a link-backed socket"))
    }

    pub fn leave_multicast_v6(&self, _multiaddr: &Ipv6Addr, _interface: u32) -> io::Result<()> {
        Err(unsupported("multicast groups are not available on a link-backed socket"))
    }

    pub fn take_error(&self) -> io::Result<Option<io::Error>> {
        self.link.take_error()
    }

    /// Link counters: datagrams dropped for a slow receiver, sent, received.
    pub fn link_stats(&self) -> LinkStats {
        self.link.stats()
    }
}

impl fmt::Debug for UdpSocket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UdpSocket")
            .field("local", &self.link.local_addr().ok())
            .field("peer", &*self.peer.lock().unwrap())
            .finish()
    }
}
