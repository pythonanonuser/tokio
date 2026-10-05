//! The host loop a hosted [`LocalEventLoop`] runs on.
//!
//! A [`LocalEventLoop`] built with [`Builder::build_hosted_local_event_loop`]
//! never parks a thread: the scheduler runs in batches from `drive`, and
//! something has to call `drive` when work appears, arm a timer for the next
//! deadline, and call `drive` again when it fires. On a JavaScript host
//! (Cloudflare Workers, Node) that something is the platform event loop.
//! Tokio does not talk to JavaScript itself. The embedding installs one
//! [`Host`] with [`install`], and every platform call goes through it.
//!
//! The contract, in full:
//!
//! * Every call into the host happens on the one thread that owns the event
//!   loops, and the host calls back ([`drive_registered`]) on that same
//!   thread. Wakers are `Send + Sync`; the host object must be too.
//! * [`Host::schedule`] runs [`drive_registered`] with the given id on a
//!   later turn of the host loop. [`Turn::Microtask`] runs before the host
//!   processes further I/O or timer callbacks of the current turn; it is
//!   what a readiness callback or a promise resolution asks for, so the
//!   woken task runs in the same host turn that woke it. [`Turn::Macrotask`]
//!   runs after the host has had a turn for its own I/O and timers; a busy
//!   scheduler asks for it between batches so it never starves the host.
//! * [`Host::set_timer`] arms the one timer the event loop owns: the next
//!   Tokio timer deadline. A new `set_timer` replaces the previous arm; the
//!   loop calls [`Host::clear_timer`] first. When it fires the host calls
//!   [`drive_registered`].
//! * A spurious [`drive_registered`] is harmless. A drive that finds nothing
//!   to do returns at once.
//! * [`Host::keepalive`] tells a host that exits when idle (Node) whether the
//!   loop still owns live tasks. A host that owns the process lifetime
//!   (Workers) ignores it.
//!
//! The ids are small integers. The host needs no callback objects, which is
//! why nothing here is `unsafe` and nothing crosses the FFI boundary but
//! integers.
//!
//! [`LocalEventLoop`]: crate::runtime::LocalEventLoop
//! [`Builder::build_hosted_local_event_loop`]: crate::runtime::Builder::build_hosted_local_event_loop

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::{Rc, Weak};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use super::event_loop::Shared;

/// Which turn of the host loop a drive should run on. See the module docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Turn {
    /// Before further I/O and timer callbacks of the current host turn.
    Microtask,
    /// After the host has had a turn of its own.
    Macrotask,
}

/// Identifies one hosted event loop to the host. Opaque; the host passes it
/// back to [`drive_registered`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EventLoopId(pub u64);

/// The platform loop. One is installed per process with [`install`].
pub trait Host: Send + Sync + 'static {
    /// Run [`drive_registered`]`(id)` on a later turn.
    fn schedule(&self, id: EventLoopId, turn: Turn);
    /// Arm the loop's timer: run [`drive_registered`]`(id)` after `after`.
    /// Replaces any previous arm for this id.
    fn set_timer(&self, id: EventLoopId, after: Duration);
    /// Disarm the loop's timer, if armed.
    fn clear_timer(&self, id: EventLoopId);
    /// Whether the loop still has live tasks. Default: ignored.
    fn keepalive(&self, id: EventLoopId, held: bool) {
        let _ = (id, held);
    }
}

static HOST: OnceLock<Arc<dyn Host>> = OnceLock::new();
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

thread_local! {
    static LOOPS: RefCell<HashMap<u64, Weak<Shared>>> = RefCell::new(HashMap::new());
}

/// Installs the process's host. Returns the host back if one is installed
/// already; the first installation wins.
pub fn install(host: Arc<dyn Host>) -> Result<(), Arc<dyn Host>> {
    HOST.set(host)
}

/// The installed host, if any.
pub fn installed() -> Option<Arc<dyn Host>> {
    HOST.get().cloned()
}

pub(crate) fn register(shared: &Rc<Shared>) -> EventLoopId {
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    LOOPS.with(|loops| {
        loops.borrow_mut().insert(id, Rc::downgrade(shared));
    });
    EventLoopId(id)
}

pub(crate) fn unregister(id: EventLoopId) {
    // The thread-local may already be gone during thread teardown.
    let _ = LOOPS.try_with(|loops| loops.borrow_mut().remove(&id.0));
}

fn lookup(id: EventLoopId) -> Option<Rc<Shared>> {
    LOOPS
        .try_with(|loops| loops.borrow().get(&id.0).and_then(Weak::upgrade))
        .ok()
        .flatten()
}

/// The host's callback: drive the event loop `id`. Returns `false` when no
/// such loop exists on this thread (it was dropped, or the id is foreign),
/// which the host should treat as a stale arm, not an error.
pub fn drive_registered(id: EventLoopId) -> bool {
    match lookup(id) {
        Some(shared) => {
            shared.drive_from_host();
            true
        }
        None => false,
    }
}
