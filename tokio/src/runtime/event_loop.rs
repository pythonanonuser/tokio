//! A `current_thread` runtime whose wait belongs to someone else.
//!
//! A native runtime waits by parking its thread in the driver. An event loop
//! cannot: on a single-threaded JavaScript host there is no thread to park
//! and nothing would ever wake it. So a [`LocalEventLoop`] runs the scheduler
//! in batches from [`drive`], never waits, and reports two things outward:
//! that it has ready work (through a [`Waker`]) and when its next timer is
//! due. Whoever owns the wait, the *host*, calls `drive` in response.
//!
//! Two shapes:
//!
//! * [`Builder::build_local_event_loop`] takes a [`Waker`] the embedder owns.
//!   Every path that would have unparked a native runtime's thread (a spawn,
//!   a wake from outside a drive, a nearer timer) wakes that waker instead,
//!   and the embedder calls [`drive`] and arms its own timer from
//!   [`next_timer`]. Tests and native harnesses use this shape.
//! * [`Builder::build_hosted_local_event_loop`] needs no waker: the loop
//!   schedules its own drives and timers on the installed [`Host`]. This is
//!   the shape the JavaScript glue builds.
//!
//! A wake that arrives while a drive is running sets a flag and the drive
//! schedules one follow-up at its end. Every wake between drives schedules
//! its own drive, preserving the host context that woke it. The follow-up
//! after a batch that still has ready work is a macrotask, so a busy loop
//! yields a host turn between batches instead of starving the host's I/O.
//!
//! [`drive`]: LocalEventLoop::drive
//! [`next_timer`]: LocalEventLoop::next_timer
//! [`Host`]: crate::runtime::host::Host
//! [`Builder::build_local_event_loop`]: crate::runtime::Builder::build_local_event_loop
//! [`Builder::build_hosted_local_event_loop`]: crate::runtime::Builder::build_hosted_local_event_loop

use crate::runtime::host::{self, EventLoopId, Host, Turn};
use crate::runtime::local_runtime::LocalRuntime;
use crate::runtime::scheduler::current_thread::Panicked;
use crate::runtime::{context, Handle};
use crate::task::JoinHandle;
use crate::util::trace::SpawnMeta;

use std::cell::Cell;
use std::future::Future;
use std::io;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Wake, Waker};
use std::thread::ThreadId;
use std::time::Duration;

/// A [`LocalRuntime`] driven from the outside instead of by parking a thread.
///
/// See the [module docs](self) for the two shapes and the wake contract.
/// Like `LocalRuntime` it is `!Send`: it is driven on the thread that built
/// it. Dropping it shuts the runtime down as dropping a `LocalRuntime` does;
/// the waker may be woken once more during the drop.
#[derive(Debug)]
pub struct LocalEventLoop {
    shared: Rc<Shared>,
}

/// Wake-side state, shared with the waker. Plain atomics: the waker is
/// `Send + Sync` without any unsafe claim about the host's threading.
#[derive(Debug, Default)]
struct Flags {
    /// A drive is on the stack.
    in_drive: AtomicBool,
    /// A wake arrived during the drive; one follow-up is owed.
    woken: AtomicBool,
}

#[derive(Debug)]
pub(crate) struct Shared {
    /// Its shutdown needs the scheduler core back, which `Drop` arranges.
    runtime: LocalRuntime,
    handle: Handle,
    flags: Arc<Flags>,
    /// `Some` for a hosted loop.
    host: Option<Arc<dyn Host>>,
    id: Cell<Option<EventLoopId>>,
    /// Whether the host's timer is armed.
    armed: Cell<bool>,
    held: Cell<bool>,
    tid: ThreadId,
    #[cfg(all(feature = "net", any(all(target_os = "emscripten", not(target_feature = "atomics")), tokio_host_net)))]
    network_dialer: std::cell::RefCell<Option<Arc<dyn crate::net::host::Dialer>>>,
}

impl std::fmt::Debug for dyn Host {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Host")
    }
}

/// The waker of a hosted loop: a wake schedules a drive on the host loop.
struct Hosted {
    host: Arc<dyn Host>,
    id: EventLoopId,
    flags: Arc<Flags>,
}

impl Wake for Hosted {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if self.flags.in_drive.load(Ordering::Acquire) {
            // The drive on the stack schedules the follow-up at its end.
            self.flags.woken.store(true, Ordering::Release);
            return;
        }
        // A pending drive belongs to the host context that scheduled it.
        // Another external context must retain its own scheduled callback.
        self.host.schedule(self.id, Turn::Microtask);
    }
}

impl LocalEventLoop {
    /// `waker` is `Some` for the embedder-driven shape and `None` for the
    /// hosted shape, which needs an installed [`Host`].
    pub(crate) fn new(runtime: LocalRuntime, waker: Option<Waker>) -> io::Result<LocalEventLoop> {
        let handle = runtime.handle().clone();
        let flags = Arc::new(Flags::default());
        let host = match &waker {
            Some(_) => None,
            None => Some(host::installed().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::Unsupported,
                    "no event loop host is installed; call tokio::runtime::host::install first \
                     or build the loop with a waker",
                )
            })?),
        };
        let shared = Rc::new(Shared {
            runtime,
            handle,
            flags: flags.clone(),
            host,
            id: Cell::new(None),
            armed: Cell::new(false),
            held: Cell::new(false),
            tid: std::thread::current().id(),
            #[cfg(all(feature = "net", any(all(target_os = "emscripten", not(target_feature = "atomics")), tokio_host_net)))]
            network_dialer: std::cell::RefCell::new(None),
        });
        let waker = match waker {
            Some(waker) => waker,
            None => {
                let id = host::register(&shared);
                shared.id.set(Some(id));
                Waker::from(Arc::new(Hosted {
                    host: shared.host.clone().expect("hosted loop has a host"),
                    id,
                    flags,
                }))
            }
        };
        shared.handle.inner.driver().set_host(waker);
        Ok(LocalEventLoop { shared })
    }

    /// Spawns a future onto the loop. It is queued and the waker woken; it
    /// never runs before `spawn_local` returns.
    #[track_caller]
    pub fn spawn_local<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        let meta = SpawnMeta::new_unnamed(std::mem::size_of::<F>());
        // SAFETY: the loop is `!Send`, so this is the thread that built the
        // runtime, and `drive` polls only on that thread.
        unsafe { self.shared.handle.spawn_local_named(future, meta) }
    }

    /// A handle to the runtime, for `spawn` from `Send` contexts and for
    /// `Handle::enter`.
    pub fn handle(&self) -> &Handle {
        &self.shared.handle
    }

    cfg_host_net! {
        /// Sets the network dialer used while this loop polls futures.
        ///
        /// Applies to `block_on` and every task polled by `drive`, including
        /// tasks spawned with [`tokio::spawn`](crate::spawn). Existing sockets
        /// retain their links. A change takes effect on the next drive or
        /// `block_on` call. Entering the runtime with [`Handle::enter`] alone
        /// does not select this dialer.
        ///
        /// Until this is called, networking uses the process default installed
        /// with [`crate::net::host::install`].
        pub fn set_network_dialer(&self, dialer: Arc<dyn crate::net::host::Dialer>) {
            *self.shared.network_dialer.borrow_mut() = Some(dialer);
        }
    }

    /// The id a hosted loop is registered under with the [`Host`]; `None`
    /// for the embedder-driven shape.
    pub fn id(&self) -> Option<EventLoopId> {
        self.shared.id.get()
    }

    /// Runs the driver's turn (due timers) and then one batch of ready tasks,
    /// at most `event_interval` of them. Never waits. If ready work remains
    /// afterwards the waker is woken again, so the host gets a turn between
    /// batches.
    ///
    /// Call it whenever the waker was woken or the timer from [`next_timer`]
    /// fired. A drive that finds nothing to do is normal.
    ///
    /// # Panics
    ///
    /// Panics on a thread other than the one that built the loop, from inside
    /// another runtime, or when a task panicked and the runtime is configured
    /// to shut down on unhandled panics.
    ///
    /// [`next_timer`]: LocalEventLoop::next_timer
    pub fn drive(&self) {
        self.shared.drive();
    }

    /// How long until the earliest pending Tokio timer, or `None` when no
    /// timer is pending. For the embedder-driven shape: arm your own timer
    /// for this and call [`drive`](Self::drive) when it fires. Recomputed
    /// after every drive; a hosted loop arms the host's timer itself.
    pub fn next_timer(&self) -> Option<Duration> {
        self.shared.next_timer()
    }

    /// Runs `future` as far as ready work carries it, running tasks that
    /// become ready along the way, without ever waiting. Where
    /// [`Runtime::block_on`] would park, nothing here could wake the future,
    /// so it is dropped and this panics. Tasks the future spawned continue
    /// from later drives.
    ///
    /// # Panics
    ///
    /// Panics if the future is still pending once no ready work remains, and
    /// in every case [`drive`](Self::drive) panics.
    ///
    /// [`Runtime::block_on`]: crate::runtime::Runtime::block_on
    #[track_caller]
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.shared.block_on(future)
    }
}

/// Clears `in_drive` even if the batch panics, so a later drive is not
/// refused forever.
struct InDrive<'a>(&'a Flags);

impl<'a> InDrive<'a> {
    fn enter(flags: &'a Flags) -> Option<InDrive<'a>> {
        if flags.in_drive.swap(true, Ordering::AcqRel) {
            None
        } else {
            Some(InDrive(flags))
        }
    }
}

impl Drop for InDrive<'_> {
    fn drop(&mut self) {
        self.0.in_drive.store(false, Ordering::Release);
    }
}

impl Shared {
    fn check_thread(&self) {
        assert_eq!(
            std::thread::current().id(),
            self.tid,
            "a `LocalEventLoop` must be driven on the thread that built it"
        );
    }

    /// The host's drive: the same as `drive`, but a re-entrant call (the host
    /// calling back while a drive is on the stack) is folded into the drive
    /// on the stack instead of panicking.
    pub(crate) fn drive_from_host(&self) {
        if self.flags.in_drive.load(Ordering::Acquire) {
            self.flags.woken.store(true, Ordering::Release);
            return;
        }
        self.drive();
    }

    fn drive(&self) {
        self.check_thread();
        let Some(guard) = InDrive::enter(&self.flags) else {
            panic!("`LocalEventLoop::drive` called from inside a drive");
        };
        let scheduler = self.handle.inner.as_current_thread();
        let batch = context::enter_runtime(&self.handle.inner, false, |_| {
            #[cfg(all(feature = "net", any(all(target_os = "emscripten", not(target_feature = "atomics")), tokio_host_net)))]
            let _dialer = crate::net::host::enter_dialer(self.network_dialer.borrow().clone());
            self.runtime.current_thread().drive_batch(scheduler)
        });
        drop(guard);
        let busy = match batch {
            Ok(busy) => busy,
            Err(Panicked) => panic!(
                "a spawned task panicked and the runtime is configured to shut down on unhandled panic"
            ),
        };
        self.after_turn(busy);
    }

    #[track_caller]
    fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.check_thread();
        let Some(guard) = InDrive::enter(&self.flags) else {
            panic!("`LocalEventLoop::block_on` called from inside a drive");
        };
        let scheduler = self.handle.inner.as_current_thread();
        let ret = context::enter_runtime(&self.handle.inner, false, |_| {
            #[cfg(all(feature = "net", any(all(target_os = "emscripten", not(target_feature = "atomics")), tokio_host_net)))]
            let _dialer = crate::net::host::enter_dialer(self.network_dialer.borrow().clone());
            self.runtime.current_thread().block_on_ready(scheduler, future)
        });
        drop(guard);
        let (out, busy) = match ret {
            Ok(v) => v,
            Err(Panicked) => panic!(
                "a spawned task panicked and the runtime is configured to shut down on unhandled panic"
            ),
        };
        // The dropped future's timers are gone; settle the host side first.
        self.after_turn(busy);
        match out {
            Some(out) => out,
            None => panic!(
                "`LocalEventLoop::block_on` cannot wait: the future is still pending with no \
                 ready work, and its wait belongs to the host loop, so nothing could wake it \
                 from here"
            ),
        }
    }

    fn next_timer(&self) -> Option<Duration> {
        next_deadline(&self.handle).map(|(_, after)| after)
    }


    fn after_turn(&self, busy: bool) {
        let woken = self.flags.woken.swap(false, Ordering::AcqRel);
        match (&self.host, self.id.get()) {
            (Some(host), Some(id)) => {
                // The host's timer is one-shot and a drive cannot tell a timer
                // fire from a scheduled drive, so the arm is renewed after every
                // drive while a deadline exists (`set_timer` replaces the
                // previous arm) and cleared once none does.
                match next_deadline(&self.handle) {
                    Some((_, after)) => {
                        host.set_timer(id, after);
                        self.armed.set(true);
                    }
                    None => {
                        if self.armed.replace(false) {
                            host.clear_timer(id);
                        }
                    }
                }
                let alive = self.handle.inner.num_alive_tasks() > 0;
                if self.held.replace(alive) != alive {
                    host.keepalive(id, alive);
                }
                if busy || woken {
                    // A follow-up, as a macrotask: a microtask here would run
                    // before the host's own I/O and timers and a self-waking
                    // task could starve them.
                    host.schedule(id, Turn::Macrotask);
                }
            }
            _ => {
                if busy || woken {
                    self.handle.inner.driver().wake_host();
                }
            }
        }
    }
}

/// The earliest timer deadline as (tick, time until it), if any.
#[cfg(feature = "time")]
fn next_deadline(handle: &Handle) -> Option<(u64, Duration)> {
    let driver = handle.inner.driver();
    let time = driver.time.as_ref()?;
    let tick = time.next_expiration_tick()?;
    let now = time.time_source().now(&driver.clock);
    Some((tick, time.time_source().tick_to_duration(tick.saturating_sub(now))))
}

#[cfg(not(feature = "time"))]
fn next_deadline(_handle: &Handle) -> Option<(u64, Duration)> {
    None
}

impl Drop for Shared {
    fn drop(&mut self) {
        if let (Some(host), Some(id)) = (&self.host, self.id.get()) {
            if self.armed.replace(false) {
                host.clear_timer(id);
            }
            if self.held.replace(false) {
                host.keepalive(id, false);
            }
            host::unregister(id);
        }
    }
}
