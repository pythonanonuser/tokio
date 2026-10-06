//! The host-driven event loop on a native thread, with a manual host: the
//! same scheduler, timer arming and wake coalescing the JavaScript glue
//! relies on, exercised without a JavaScript engine.
#![warn(rust_2018_idioms)]
#![cfg(all(feature = "full", tokio_unstable, tokio_host_loop))]

use std::cell::Cell;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Wake, Waker};
use std::time::{Duration, Instant};

use tokio::runtime::host::{self, drive_registered, drive_timer_registered, EventLoopId, Host, Turn};
use tokio::runtime::{Builder, LocalEventLoop, LocalOptions};

#[derive(Default)]
struct State {
    micro: VecDeque<(EventLoopId, usize)>,
    macro_: VecDeque<EventLoopId>,
    timers: HashMap<EventLoopId, Instant>,
    keepalive: HashMap<EventLoopId, bool>,
    /// Per loop: microtasks scheduled, macrotasks scheduled, timers set.
    counts: HashMap<EventLoopId, (usize, usize, usize)>,
}

/// A host loop a test steps by hand. Its state is per thread: event loops
/// are registered per thread, and the test binary runs tests on several
/// threads at once, so each test's drives must land on its own thread.
#[derive(Default)]
struct ManualHost;

thread_local! {
    static STATE: Mutex<State> = Mutex::new(State::default());
    static CONTEXT: Cell<usize> = const { Cell::new(0) };
}

fn with_state<R>(f: impl FnOnce(&mut State) -> R) -> R {
    STATE.with(|s| f(&mut s.lock().unwrap()))
}

impl Host for ManualHost {
    fn schedule(&self, id: EventLoopId, turn: Turn) {
        with_state(|s| match turn {
            Turn::Microtask => {
                s.micro.push_back((id, CONTEXT.with(Cell::get)));
                s.counts.entry(id).or_default().0 += 1;
            }
            Turn::Macrotask => {
                s.macro_.push_back(id);
                s.counts.entry(id).or_default().1 += 1;
            }
        })
    }

    fn set_timer(&self, id: EventLoopId, after: Duration) {
        with_state(|s| {
            s.timers.insert(id, Instant::now() + after);
            s.counts.entry(id).or_default().2 += 1;
        });
    }

    fn clear_timer(&self, id: EventLoopId) {
        with_state(|s| s.timers.remove(&id));
    }

    fn keepalive(&self, id: EventLoopId, held: bool) {
        with_state(|s| s.keepalive.insert(id, held));
    }
}

fn the_host() -> &'static Arc<ManualHost> {
    static HOST: OnceLock<Arc<ManualHost>> = OnceLock::new();
    HOST.get_or_init(|| {
        let h = Arc::new(ManualHost);
        host::install(h.clone()).ok().expect("first install in this binary");
        h
    })
}

impl ManualHost {
    /// One step of the host loop: every queued microtask, then one macrotask,
    /// then the earliest due timer. Returns whether anything ran.
    fn step(&self) -> bool {
        let mut ran = false;
        loop {
            let next = with_state(|s| s.micro.pop_front());
            match next {
                Some((id, context)) => {
                    let previous = CONTEXT.with(|current| current.replace(context));
                    drive_registered(id);
                    CONTEXT.with(|current| current.set(previous));
                    ran = true;
                }
                None => break,
            }
        }
        let next = with_state(|s| s.macro_.pop_front());
        if let Some(id) = next {
            drive_registered(id);
            ran = true;
        }
        let due = with_state(|s| {
            let now = Instant::now();
            let due: Vec<EventLoopId> = s.timers.iter().filter(|(_, at)| **at <= now).map(|(id, _)| *id).collect();
            for id in &due {
                s.timers.remove(id);
            }
            due
        });
        for id in due {
            drive_timer_registered(id);
            ran = true;
        }
        ran
    }

    /// Fires the loop's timer now, before it is due, as a host that rounds
    /// delays to whole milliseconds can.
    fn fire_timer_early(&self, rt: &LocalEventLoop) {
        let id = rt.id().expect("a hosted loop");
        let armed = with_state(|s| s.timers.remove(&id).is_some());
        assert!(armed, "the host timer is armed");
        drive_timer_registered(id);
    }

    /// Steps until `done()` or the deadline, sleeping until the next timer
    /// when nothing else is queued.
    fn run_until(&self, mut done: impl FnMut() -> bool, deadline: Duration) {
        let end = Instant::now() + deadline;
        while !done() {
            assert!(Instant::now() < end, "the host loop did not finish within {deadline:?}");
            if !self.step() {
                let next = with_state(|s| s.timers.values().min().copied());
                match next {
                    Some(at) => std::thread::sleep(at.saturating_duration_since(Instant::now()).min(Duration::from_millis(20))),
                    None => std::thread::sleep(Duration::from_millis(1)),
                }
            }
        }
    }

    fn counts(&self, rt: &LocalEventLoop) -> (usize, usize, usize) {
        let id = rt.id().expect("a hosted loop");
        with_state(|s| s.counts.get(&id).copied().unwrap_or_default())
    }

    fn idle(&self, rt: &LocalEventLoop) -> bool {
        let id = rt.id().expect("a hosted loop");
        with_state(|s| !s.micro.iter().any(|(queued, _)| *queued == id) && !s.macro_.contains(&id))
    }
}

fn hosted() -> LocalEventLoop {
    the_host();
    Builder::new_current_thread()
        .enable_all()
        .build_hosted_local_event_loop(LocalOptions::default())
        .expect("hosted event loop")
}

#[test]
fn spawned_tasks_run_from_drives_and_timers_arm_the_host() {
    let rt = hosted();
    let done = Arc::new(AtomicUsize::new(0));
    let out = Arc::new(Mutex::new(Vec::new()));
    let (d, o) = (done.clone(), out.clone());
    rt.spawn_local(async move {
        let handles: Vec<_> = (1..=3u32)
            .map(|i| tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(10 * i as u64)).await;
                i * i
            }))
            .collect();
        for h in handles {
            o.lock().unwrap().push(h.await.unwrap());
        }
        d.store(1, Ordering::SeqCst);
    });
    // Nothing ran yet: spawn_local only queues and wakes.
    assert_eq!(done.load(Ordering::SeqCst), 0);
    let started = Instant::now();
    the_host().run_until(|| done.load(Ordering::SeqCst) == 1, Duration::from_secs(5));
    assert_eq!(*out.lock().unwrap(), vec![1, 4, 9]);
    let elapsed = started.elapsed();
    assert!(elapsed >= Duration::from_millis(30) && elapsed < Duration::from_millis(500), "{elapsed:?}");
    let (_, _, timer_sets) = the_host().counts(&rt);
    assert!(timer_sets >= 1, "the loop armed the host timer for its sleeps");
    drop(rt);
}

#[test]
fn a_wake_from_outside_a_drive_schedules_one_microtask() {
    let rt = hosted();
    let (tx, rx) = tokio::sync::oneshot::channel::<u32>();
    let got = Arc::new(AtomicUsize::new(0));
    let g = got.clone();
    rt.spawn_local(async move {
        g.store(rx.await.unwrap() as usize, Ordering::SeqCst);
    });
    // Run the spawn: the task registers on the channel and goes idle.
    the_host().run_until(|| the_host().idle(&rt), Duration::from_secs(1));
    let (micro_before, _, _) = the_host().counts(&rt);
    // The send comes from outside any drive (this thread, no runtime entered).
    tx.send(7).unwrap();
    let (micro_after, _, _) = the_host().counts(&rt);
    assert_eq!(micro_after - micro_before, 1, "exactly one microtask drive for the wake");
    the_host().run_until(|| got.load(Ordering::SeqCst) == 7, Duration::from_secs(1));
    drop(rt);
}

#[test]
fn repeated_notifications_of_one_waiter_need_one_drive() {
    let rt = hosted();
    let notify = Arc::new(tokio::sync::Notify::new());
    let count = Arc::new(AtomicUsize::new(0));
    let (n, c) = (notify.clone(), count.clone());
    rt.spawn_local(async move {
        loop {
            n.notified().await;
            c.fetch_add(1, Ordering::SeqCst);
        }
    });
    the_host().run_until(|| the_host().idle(&rt), Duration::from_secs(1));
    let (micro_before, _, _) = the_host().counts(&rt);
    for _ in 0..100 {
        notify.notify_one();
    }
    let (micro_after, _, _) = the_host().counts(&rt);
    // Notify wakes the registered waiter once and retains one permit.
    assert_eq!(micro_after - micro_before, 1, "one waiter was woken");
    the_host().run_until(|| count.load(Ordering::SeqCst) >= 1, Duration::from_secs(1));
    drop(rt);
}

#[test]
fn wakes_of_two_waiters_between_drives_fold_into_one_drive() {
    // Two tasks wait on two channels. Both are woken from outside any drive,
    // from two host callbacks of the same owner, before either drive runs:
    // one microtask drive is scheduled and it serves both wakes.
    let rt = hosted();
    let completed = Arc::new(AtomicUsize::new(0));
    let mut senders = Vec::new();
    for _ in 0..2 {
        let (tx, rx) = tokio::sync::oneshot::channel();
        senders.push(tx);
        let completed = completed.clone();
        rt.spawn_local(async move {
            rx.await.unwrap();
            completed.fetch_add(1, Ordering::SeqCst);
        });
    }
    the_host().run_until(|| the_host().idle(&rt), Duration::from_secs(1));
    let (micro_before, _, _) = the_host().counts(&rt);
    for (index, sender) in senders.into_iter().enumerate() {
        CONTEXT.with(|context| context.set(index + 1));
        sender.send(()).unwrap();
    }
    CONTEXT.with(|context| context.set(0));
    let (micro_after, _, _) = the_host().counts(&rt);
    assert_eq!(micro_after - micro_before, 1, "two wakes between drives, one scheduled drive");
    the_host().run_until(|| completed.load(Ordering::SeqCst) == 2, Duration::from_secs(1));
    drop(rt);
}

#[test]
fn reentrant_host_drives_schedule_one_follow_up() {
    the_host();
    let rt = Builder::new_current_thread()
        .event_interval(1)
        .build_hosted_local_event_loop(LocalOptions::default())
        .unwrap();
    let id = rt.id().unwrap();
    rt.spawn_local(async move {
        for _ in 0..100 {
            // Reentrant host calls fold into the drive already on the stack.
            assert!(drive_registered(id));
        }
    });
    let (micro_before, macro_before, _) = the_host().counts(&rt);
    drive_registered(id);
    let (micro_after, macro_after, _) = the_host().counts(&rt);
    assert_eq!(micro_after, micro_before);
    assert_eq!(macro_after - macro_before, 1);
}

#[test]
fn a_busy_loop_yields_a_macrotask_between_batches() {
    let rt = {
        the_host();
        Builder::new_current_thread()
            .enable_all()
            .event_interval(4)
            .build_hosted_local_event_loop(LocalOptions::default())
            .unwrap()
    };
    let done = Arc::new(AtomicUsize::new(0));
    let d = done.clone();
    rt.spawn_local(async move {
        for _ in 0..200 {
            tokio::task::yield_now().await;
        }
        d.store(1, Ordering::SeqCst);
    });
    let (_, macro_before, _) = the_host().counts(&rt);
    the_host().run_until(|| done.load(Ordering::SeqCst) == 1, Duration::from_secs(5));
    let (_, macro_after, _) = the_host().counts(&rt);
    assert!(macro_after - macro_before >= 10, "batches of four yields handed the host turns: {}", macro_after - macro_before);
    drop(rt);
}

#[test]
fn block_on_runs_a_ready_future_and_panics_when_it_would_wait() {
    let rt = hosted();
    let v = rt.block_on(async { 41 + 1 });
    assert_eq!(v, 42);
    let waits = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        rt.block_on(async { tokio::time::sleep(Duration::from_secs(10)).await });
    }));
    assert!(waits.is_err(), "a future that must wait cannot be block_on'd on an event loop");
    drop(rt);
}

#[test]
fn keepalive_follows_live_tasks() {
    let rt = hosted();
    let done = Arc::new(AtomicUsize::new(0));
    let d = done.clone();
    rt.spawn_local(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        d.store(1, Ordering::SeqCst);
    });
    the_host().run_until(|| the_host().idle(&rt), Duration::from_secs(1));
    let id = rt.id().unwrap();
    assert_eq!(with_state(|s| s.keepalive[&id]), true, "held while a task sleeps");
    the_host().run_until(|| done.load(Ordering::SeqCst) == 1, Duration::from_secs(2));
    // The completing drive released the hold.
    the_host().run_until(|| !with_state(|s| s.keepalive[&id]), Duration::from_secs(1));
    drop(rt);
}

struct Counting(AtomicUsize);
impl Wake for Counting {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn the_embedder_driven_shape_reports_wakes_and_the_next_timer() {
    let counting = Arc::new(Counting(AtomicUsize::new(0)));
    let rt = Builder::new_current_thread()
        .enable_all()
        .build_local_event_loop(LocalOptions::default(), Waker::from(counting.clone()))
        .unwrap();
    let done = Arc::new(AtomicUsize::new(0));
    let d = done.clone();
    rt.spawn_local(async move {
        tokio::time::sleep(Duration::from_millis(30)).await;
        d.store(1, Ordering::SeqCst);
    });
    assert!(counting.0.load(Ordering::SeqCst) >= 1, "spawn woke the embedder");
    rt.drive();
    let next = rt.next_timer().expect("a sleep is pending");
    assert!(next <= Duration::from_millis(31), "{next:?}");
    std::thread::sleep(next + Duration::from_millis(2));
    rt.drive();
    assert_eq!(done.load(Ordering::SeqCst), 1);
    assert!(rt.next_timer().is_none());
}

#[test]
fn drives_that_leave_the_deadline_in_place_do_not_arm_the_timer_again() {
    let host = the_host();
    let rt = hosted();
    let done = Arc::new(AtomicUsize::new(0));
    // One sleep sets the deadline; a channel then wakes the loop many times
    // before it is due.
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<u32>();
    let d = done.clone();
    rt.spawn_local(async move {
        tokio::time::sleep(Duration::from_millis(150)).await;
        d.fetch_add(1, Ordering::SeqCst);
    });
    let d = done.clone();
    rt.spawn_local(async move {
        let mut n = 0;
        while let Some(_) = rx.recv().await {
            n += 1;
            if n == 50 {
                break;
            }
        }
        d.fetch_add(1, Ordering::SeqCst);
    });
    for _ in 0..50 {
        tx.send(1).unwrap();
        host.run_until(|| host.idle(&rt), Duration::from_secs(2));
    }
    let (_, _, timers_before_sleep_fires) = host.counts(&rt);
    assert_eq!(timers_before_sleep_fires, 1, "fifty wakes with the same deadline pending arm the host once");
    host.run_until(|| done.load(Ordering::SeqCst) == 2, Duration::from_secs(5));
}

#[test]
fn a_timer_the_host_fired_early_is_armed_again() {
    let host = the_host();
    let rt = hosted();
    let done = Arc::new(AtomicUsize::new(0));
    let d = done.clone();
    rt.spawn_local(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        d.fetch_add(1, Ordering::SeqCst);
    });
    host.run_until(|| host.idle(&rt), Duration::from_secs(2));
    let (_, _, armed_once) = host.counts(&rt);
    assert_eq!(armed_once, 1);
    // The host fires its timer long before the deadline: the drive finds the
    // sleep still pending and must arm the host again, or the sleep never ends.
    host.fire_timer_early(&rt);
    let (_, _, armed_twice) = host.counts(&rt);
    assert_eq!(armed_twice, 2, "an early fire re-arms");
    host.run_until(|| done.load(Ordering::SeqCst) == 1, Duration::from_secs(5));
}
