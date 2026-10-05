//! The link-backed `tokio::net` types against an in-memory dialer, on the
//! multi-thread runtime: the socket API, datagram boundaries, readiness and
//! wakeups, and the `Send + Sync` the types promise.
#![warn(rust_2018_idioms)]
#![cfg(all(feature = "full", tokio_unstable, tokio_host_net))]

use std::collections::VecDeque;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll, Waker};

use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadBuf};
use tokio::net::host::{
    install, resolve, DialFuture, Dialer, Link, LinkStats, Option_, ResolveFuture, Target, TcpOptions, TcpStream, UdpOptions, UdpSocket,
};

#[derive(Default)]
struct Bytes {
    buf: VecDeque<u8>,
    closed: bool,
    waker: Option<Waker>,
}

#[derive(Default)]
struct Datagrams {
    queue: VecDeque<(SocketAddr, Vec<u8>)>,
    waker: Option<Waker>,
}

/// One in-memory link; the test holds the far end.
#[derive(Default)]
struct MemLink {
    kind: Mutex<Option<Target>>,
    to_client: Mutex<Bytes>,
    to_server: Mutex<Bytes>,
    inbound: Mutex<Datagrams>,
    outbound: Mutex<Vec<(Option<SocketAddr>, Vec<u8>)>>,
    peer: Mutex<Option<SocketAddr>>,
    local: Mutex<Option<SocketAddr>>,
    options: Mutex<Vec<Option_>>,
    shut: Mutex<bool>,
}

impl MemLink {
    fn feed(&self, data: &[u8]) {
        let mut b = self.to_client.lock().unwrap();
        b.buf.extend(data);
        if let Some(w) = b.waker.take() {
            w.wake();
        }
    }
    fn close_from_far_end(&self) {
        let mut b = self.to_client.lock().unwrap();
        b.closed = true;
        if let Some(w) = b.waker.take() {
            w.wake();
        }
    }
    fn written(&self) -> Vec<u8> {
        self.to_server.lock().unwrap().buf.iter().copied().collect()
    }
    fn deliver(&self, from: SocketAddr, data: &[u8]) {
        let mut d = self.inbound.lock().unwrap();
        d.queue.push_back((from, data.to_vec()));
        if let Some(w) = d.waker.take() {
            w.wake();
        }
    }
}

impl Link for MemLink {
    fn poll_read(&self, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        let mut b = self.to_client.lock().unwrap();
        if b.buf.is_empty() {
            if b.closed {
                return Poll::Ready(Ok(()));
            }
            b.waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let n = buf.remaining().min(b.buf.len());
        for _ in 0..n {
            buf.put_slice(&[b.buf.pop_front().unwrap()]);
        }
        Poll::Ready(Ok(()))
    }
    fn poll_write(&self, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        if *self.shut.lock().unwrap() {
            return Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, "shut down")));
        }
        self.to_server.lock().unwrap().buf.extend(buf);
        Poll::Ready(Ok(buf.len()))
    }
    fn poll_flush(&self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(&self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        *self.shut.lock().unwrap() = true;
        Poll::Ready(Ok(()))
    }
    fn poll_read_ready(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut b = self.to_client.lock().unwrap();
        let mut d = self.inbound.lock().unwrap();
        if !b.buf.is_empty() || b.closed || !d.queue.is_empty() {
            return Poll::Ready(Ok(()));
        }
        b.waker = Some(cx.waker().clone());
        d.waker = Some(cx.waker().clone());
        Poll::Pending
    }
    fn poll_write_ready(&self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn try_read(&self, buf: &mut [u8]) -> io::Result<usize> {
        let mut b = self.to_client.lock().unwrap();
        if b.buf.is_empty() {
            return if b.closed { Ok(0) } else { Err(io::ErrorKind::WouldBlock.into()) };
        }
        let n = buf.len().min(b.buf.len());
        for slot in buf.iter_mut().take(n) {
            *slot = b.buf.pop_front().unwrap();
        }
        Ok(n)
    }
    fn try_write(&self, buf: &[u8]) -> io::Result<usize> {
        self.to_server.lock().unwrap().buf.extend(buf);
        Ok(buf.len())
    }
    fn poll_recv_from(&self, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<SocketAddr>> {
        let mut d = self.inbound.lock().unwrap();
        let peer = *self.peer.lock().unwrap();
        // A connected socket drops datagrams from other peers, like the kernel.
        while let Some((from, _)) = d.queue.front() {
            if peer.map_or(true, |p| p == *from) {
                break;
            }
            d.queue.pop_front();
        }
        match d.queue.pop_front() {
            Some((from, data)) => {
                let n = buf.remaining().min(data.len());
                buf.put_slice(&data[..n]);
                Poll::Ready(Ok(from))
            }
            None => {
                d.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
    fn poll_send_to(&self, _cx: &mut Context<'_>, buf: &[u8], target: Option<SocketAddr>) -> Poll<io::Result<usize>> {
        if target.is_none() && self.peer.lock().unwrap().is_none() {
            return Poll::Ready(Err(io::Error::new(io::ErrorKind::NotConnected, "no peer")));
        }
        self.outbound.lock().unwrap().push((target, buf.to_vec()));
        Poll::Ready(Ok(buf.len()))
    }
    fn try_recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        let mut d = self.inbound.lock().unwrap();
        match d.queue.pop_front() {
            Some((from, data)) => {
                let n = buf.len().min(data.len());
                buf[..n].copy_from_slice(&data[..n]);
                Ok((n, from))
            }
            None => Err(io::ErrorKind::WouldBlock.into()),
        }
    }
    fn try_send_to(&self, buf: &[u8], target: Option<SocketAddr>) -> io::Result<usize> {
        self.outbound.lock().unwrap().push((target, buf.to_vec()));
        Ok(buf.len())
    }
    fn connect_peer(&self, peer: SocketAddr) -> io::Result<()> {
        *self.peer.lock().unwrap() = Some(peer);
        Ok(())
    }
    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.local.lock().unwrap().unwrap_or_else(|| "10.0.0.1:40000".parse().unwrap()))
    }
    fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.peer.lock().unwrap().ok_or_else(|| io::ErrorKind::NotConnected.into())
    }
    fn set_option(&self, option: Option_) -> io::Result<()> {
        self.options.lock().unwrap().push(option);
        Ok(())
    }
    fn take_error(&self) -> io::Result<Option<io::Error>> {
        Ok(None)
    }
    fn stats(&self) -> LinkStats {
        LinkStats::default()
    }
}

#[derive(Default)]
struct MemDialer {
    links: Mutex<Vec<Arc<MemLink>>>,
}

impl Dialer for MemDialer {
    fn connect_tcp(&self, target: Target, _options: TcpOptions) -> DialFuture {
        let link = Arc::new(MemLink::default());
        *link.kind.lock().unwrap() = Some(target.clone());
        if let Target::Addr(a) = target {
            *link.peer.lock().unwrap() = Some(a);
        }
        self.links.lock().unwrap().push(link.clone());
        Box::pin(async move { Ok(link as Arc<dyn Link>) })
    }
    fn bind_udp(&self, local: SocketAddr, _options: UdpOptions) -> DialFuture {
        let link = Arc::new(MemLink::default());
        *link.local.lock().unwrap() = Some(local);
        self.links.lock().unwrap().push(link.clone());
        Box::pin(async move { Ok(link as Arc<dyn Link>) })
    }
    fn resolve(&self, host: String, port: u16) -> ResolveFuture {
        Box::pin(async move {
            if host == "example.test" {
                Ok(vec![SocketAddr::new([93, 184, 216, 34].into(), port)])
            } else {
                Err(io::Error::new(io::ErrorKind::NotFound, "no such host"))
            }
        })
    }
}

fn the_dialer() -> &'static Arc<MemDialer> {
    static DIALER: OnceLock<Arc<MemDialer>> = OnceLock::new();
    DIALER.get_or_init(|| {
        let d = Arc::new(MemDialer::default());
        install(d.clone()).ok().expect("first install in this binary");
        d
    })
}

/// The link a test opened: tests run on several threads at once, so a link is
/// found by the address or name that test alone uses.
fn link_for(target: Target) -> Arc<MemLink> {
    the_dialer()
        .links
        .lock()
        .unwrap()
        .iter()
        .find(|l| *l.kind.lock().unwrap() == Some(target.clone()))
        .cloned()
        .expect("the link this test opened")
}

fn udp_link_for(local: SocketAddr) -> Arc<MemLink> {
    the_dialer()
        .links
        .lock()
        .unwrap()
        .iter()
        .find(|l| *l.local.lock().unwrap() == Some(local))
        .cloned()
        .expect("the socket this test bound")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_reads_writes_and_half_closes_over_the_link() {
    the_dialer();
    let mut stream = TcpStream::connect("192.0.2.10:443".parse::<SocketAddr>().unwrap()).await.unwrap();
    let link = link_for(Target::Addr("192.0.2.10:443".parse().unwrap()));
    assert_eq!(stream.peer_addr().unwrap(), "192.0.2.10:443".parse().unwrap());

    stream.write_all(b"GET / HTTP/1.0\r\n\r\n").await.unwrap();
    assert_eq!(link.written(), b"GET / HTTP/1.0\r\n\r\n");

    // A read waits until the far end has something, then returns it.
    let reader = tokio::spawn(async move {
        let mut buf = vec![0u8; 64];
        let n = stream.read(&mut buf).await.unwrap();
        buf.truncate(n);
        (stream, buf)
    });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    link.feed(b"HTTP/1.0 200 OK\r\n");
    let (mut stream, got) = reader.await.unwrap();
    assert_eq!(got, b"HTTP/1.0 200 OK\r\n");

    // EOF from the far end ends reads; shutdown half-closes our side.
    link.close_from_far_end();
    let mut rest = Vec::new();
    stream.read_to_end(&mut rest).await.unwrap();
    assert!(rest.is_empty());
    stream.shutdown().await.unwrap();
    assert!(stream.write_all(b"x").await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_split_halves_work_from_different_tasks() {
    the_dialer();
    let stream = TcpStream::connect("192.0.2.11:80".parse::<SocketAddr>().unwrap()).await.unwrap();
    let link = link_for(Target::Addr("192.0.2.11:80".parse().unwrap()));
    let (mut rd, mut wr) = stream.into_split();
    let writer = tokio::spawn(async move {
        wr.write_all(b"ping").await.unwrap();
        wr
    });
    let reader = tokio::spawn(async move {
        let mut buf = [0u8; 4];
        rd.read_exact(&mut buf).await.unwrap();
        (rd, buf)
    });
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    link.feed(b"pong");
    let wr = writer.await.unwrap();
    let (rd, buf) = reader.await.unwrap();
    assert_eq!(&buf, b"pong");
    assert_eq!(link.written(), b"ping");
    let stream = rd.reunite(wr).unwrap();
    stream.set_nodelay(true).unwrap();
    assert_eq!(link.options.lock().unwrap().as_slice(), &[Option_::Nodelay(true)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn udp_keeps_datagram_boundaries_and_sources() {
    the_dialer();
    let socket = UdpSocket::bind("0.0.0.0:40001".parse::<SocketAddr>().unwrap()).await.unwrap();
    let link = udp_link_for("0.0.0.0:40001".parse().unwrap());
    let a: SocketAddr = "203.0.113.5:40120".parse().unwrap();
    let b: SocketAddr = "203.0.113.6:40121".parse().unwrap();

    socket.send_to(b"status", a).await.unwrap();
    assert_eq!(link.outbound.lock().unwrap().as_slice(), &[(Some(a), b"status".to_vec())]);

    link.deliver(a, &[1u8; 1408]);
    link.deliver(b, b"small");
    let mut buf = [0u8; 2048];
    let (n, from) = socket.recv_from(&mut buf).await.unwrap();
    assert_eq!((n, from), (1408, a));
    let (n, from) = socket.recv_from(&mut buf).await.unwrap();
    assert_eq!((n, from), (5, b));
    assert_eq!(&buf[..5], b"small");

    // A datagram larger than the buffer is truncated, as recvfrom does.
    link.deliver(a, &[2u8; 100]);
    let mut short = [0u8; 10];
    let (n, _) = socket.recv_from(&mut short).await.unwrap();
    assert_eq!(n, 10);
    assert!(socket.try_recv_from(&mut buf).unwrap_err().kind() == io::ErrorKind::WouldBlock);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn udp_readable_wakes_when_a_datagram_arrives_from_another_thread() {
    the_dialer();
    let socket = Arc::new(UdpSocket::bind("0.0.0.0:40002".parse::<SocketAddr>().unwrap()).await.unwrap());
    let link = udp_link_for("0.0.0.0:40002".parse().unwrap());
    let s = socket.clone();
    let waiter = tokio::spawn(async move {
        s.readable().await.unwrap();
        let mut buf = [0u8; 16];
        s.try_recv_from(&mut buf).unwrap()
    });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(!waiter.is_finished(), "nothing arrived yet");
    let from: SocketAddr = "198.51.100.9:5000".parse().unwrap();
    std::thread::spawn(move || link.deliver(from, b"rtp"))
        .join()
        .unwrap();
    let (n, src) = tokio::time::timeout(std::time::Duration::from_secs(1), waiter).await.unwrap().unwrap();
    assert_eq!((n, src), (3, from));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn udp_connect_sets_the_default_peer_and_filters_others() {
    the_dialer();
    let socket = UdpSocket::bind("0.0.0.0:40003".parse::<SocketAddr>().unwrap()).await.unwrap();
    let link = udp_link_for("0.0.0.0:40003".parse().unwrap());
    let voice: SocketAddr = "66.22.0.1:50001".parse().unwrap();
    let other: SocketAddr = "66.22.0.2:50002".parse().unwrap();
    assert_eq!(socket.send(b"x").await.unwrap_err().kind(), io::ErrorKind::NotConnected);
    socket.connect(voice).await.unwrap();
    assert_eq!(socket.peer_addr().unwrap(), voice);
    socket.send(b"discovery").await.unwrap();
    assert_eq!(link.outbound.lock().unwrap().last().unwrap(), &(None, b"discovery".to_vec()));
    link.deliver(other, b"stray");
    link.deliver(voice, b"reply");
    let mut buf = [0u8; 16];
    let n = socket.recv(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"reply", "the stray peer's datagram was filtered");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn names_resolve_through_the_dialer() {
    the_dialer();
    let addrs = resolve("example.test".into(), 80).await.unwrap();
    assert_eq!(addrs, vec!["93.184.216.34:80".parse().unwrap()]);
    assert_eq!(resolve("nope.test".into(), 80).await.unwrap_err().kind(), io::ErrorKind::NotFound);
    // An address literal needs no round trip.
    let addrs = resolve("127.0.0.1".into(), 9).await.unwrap();
    assert_eq!(addrs, vec!["127.0.0.1:9".parse().unwrap()]);
    let stream = TcpStream::connect_name("gateway.discord.gg", 443).await.unwrap();
    let link = link_for(Target::Name("gateway.discord.gg".into(), 443));
    assert_eq!(link.peer_addr().unwrap_err().kind(), io::ErrorKind::NotConnected, "a name target has no peer address until the link layer reports one");
    drop(stream);
}

#[test]
fn the_types_are_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<TcpStream>();
    assert_send_sync::<UdpSocket>();
    assert_send_sync::<tokio::net::host::OwnedReadHalf>();
    assert_send_sync::<tokio::net::host::OwnedWriteHalf>();
}
