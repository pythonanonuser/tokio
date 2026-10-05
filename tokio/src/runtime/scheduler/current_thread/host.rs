//! Running the `current_thread` scheduler without parking: the batches a
//! [`LocalEventLoop`](crate::runtime::LocalEventLoop) is made of.
//!
//! A batch is the driver's turn (due timers, with a zero wait) followed by
//! up to `event_interval` tasks, exactly the slice of `block_on`'s loop that
//! runs between two parks. Nothing here waits; where `block_on` would park,
//! a batch returns and reports whether ready work remains.

use super::{Context, Core, CoreGuard, CurrentThread, Handle};
use crate::loom::sync::Arc;

use std::future::Future;
use std::task::Poll::Ready;
use std::thread;

/// A task panicked and the runtime is configured to shut down on unhandled
/// panics; the caller turns this into the panic `block_on` would raise.
#[derive(Debug)]
pub(crate) struct Panicked;

impl CurrentThread {
    /// One batch. Must be called inside `enter_runtime`. Returns whether
    /// ready work remains queued, so the caller knows to drive again.
    pub(crate) fn drive_batch(&self, handle: &Arc<Handle>) -> Result<bool, Panicked> {
        let core = self
            .take_core(handle)
            .expect("the scheduler core is checked out; a `block_on` is active");
        handle
            .shared
            .worker_metrics
            .set_thread_id(thread::current().id());
        core.drive_batch()
    }

    /// Polls `future` as far as ready work carries it. Returns the output if
    /// it completed, `None` where a native `block_on` would have parked (the
    /// future is dropped), and whether ready work remains. Must be called
    /// inside `enter_runtime`.
    pub(crate) fn block_on_ready<F: Future>(
        &self,
        handle: &Arc<Handle>,
        future: F,
    ) -> Result<(Option<F::Output>, bool), Panicked> {
        let core = self
            .take_core(handle)
            .expect("the scheduler core is checked out; a `block_on` is active");
        handle
            .shared
            .worker_metrics
            .set_thread_id(thread::current().id());
        core.block_on_ready(future)
    }
}

enum Batch {
    /// `event_interval` tasks ran; more may be queued.
    Interval,
    /// The queues ran dry.
    Exhausted,
    /// A task panicked and the runtime shuts down.
    Panicked,
}

impl Core {
    /// Ready work in either queue, or a deferred waker (`yield_now`) that
    /// the batch released into the queue.
    fn has_ready_work(&self, handle: &Handle) -> bool {
        !self.tasks.is_empty() || handle.shared.inject.len() > 0
    }
}

impl Context {
    /// The driver's turn with a zero wait (due timers fire, nothing blocks),
    /// then up to `event_interval` tasks.
    fn run_batch(&self, core: Box<Core>) -> (Box<Core>, Batch) {
        let handle = &self.handle;
        let mut core = self.park_yield(core, handle);
        core.metrics.start_processing_scheduled_tasks();

        let interval = handle.shared.config.event_interval;
        let mut ran = 0;
        let mut exhausted = false;
        while ran < interval {
            if core.unhandled_panic {
                core.metrics.end_processing_scheduled_tasks();
                return (core, Batch::Panicked);
            }
            core.tick();
            let Some(task) = core.next_task(handle) else {
                exhausted = true;
                break;
            };
            let task = handle.shared.owned.assert_owner(task);
            core = self.run_task(task, core);
            ran += 1;
        }
        core.metrics.end_processing_scheduled_tasks();
        // Deferred wakers (`yield_now`) are released where a park would be;
        // their schedules need the core in the context to land in the queue.
        let (core, ()) = self.enter(core, || self.defer.wake());
        let batch = if exhausted {
            Batch::Exhausted
        } else {
            Batch::Interval
        };
        (core, batch)
    }
}

impl CoreGuard<'_> {
    fn drive_batch(self) -> Result<bool, Panicked> {
        let busy = self.enter(|core, context| {
            let (core, batch) = context.run_batch(core);
            let busy = match batch {
                Batch::Panicked => None,
                Batch::Interval => Some(true),
                Batch::Exhausted => Some(core.has_ready_work(&context.handle)),
            };
            (core, busy)
        });
        busy.ok_or(Panicked)
    }

    fn block_on_ready<F: Future>(self, future: F) -> Result<(Option<F::Output>, bool), Panicked> {
        let ret = self.enter(|mut core, context| {
            let waker = Handle::waker_ref(&context.handle);
            let mut cx = std::task::Context::from_waker(&waker);
            pin!(future);
            loop {
                let handle = &context.handle;
                if handle.reset_woken() {
                    let (c, res) = context.enter(core, || {
                        crate::task::coop::budget(|| future.as_mut().poll(&mut cx))
                    });
                    core = c;
                    if let Ready(v) = res {
                        let busy = core.has_ready_work(handle);
                        return (core, Some((Some(v), busy)));
                    }
                }
                let (c, batch) = context.run_batch(core);
                core = c;
                match batch {
                    Batch::Panicked => return (core, None),
                    Batch::Interval => {}
                    Batch::Exhausted => {
                        if !core.has_ready_work(handle) && !handle.shared.woken.load(std::sync::atomic::Ordering::Acquire) {
                            return (core, Some((None, false)));
                        }
                    }
                }
            }
        });
        ret.ok_or(Panicked)
    }
}
