//! The host-driven event loop on a native thread, with a manual host: the
//! same scheduler, timer arming and wake coalescing the JavaScript glue
//! relies on, exercised without a JavaScript engine.
#![warn(rust_2018_idioms)]
#![cfg(all(feature = "full", tokio_unstable, tokio_host_loop))]

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Wake, Waker};
use std::time::{Duration, Instant};

use tokio::runtime::host::{self, drive_registered, EventLoopId, Host, Turn};
use tokio::runtime::{Builder, LocalEventLoop, LocalOptions};

#[derive(Default)]
struct State {
    micro: VecDeque<EventLoopId>,
    macro_: VecDeque<EventLoopId>,
    timers: HashMap<EventLoopId, Instant>,
    keepalive: HashMap<EventLoopId, bool>,
    scheduled_micro: usize,
    scheduled_macro: usize,
    timer_sets: usize,
}

/// A host loop a test steps by hand.
#[derive(Default)]
struct ManualHost {
    state: Mutex<State>,
}

impl Host for ManualHost {
    fn schedule(&self, id: EventLoopId, turn: Turn) {
        let mut s = self.state.lock().unwrap();
        match turn {
            Turn::Microtask => {
                s.micro.push_back(id);
                s.scheduled_micro += 1;
            }
            Turn::Macrotask => {
                s.macro_.push_back(id);
                s.scheduled_macro += 1;
            }
        }
    }

    fn set_timer(&self, id: EventLoopId, after: Duration) {
        let mut s = self.state.lock().unwrap();
        s.timers.insert(id, Instant::now() + after);
        s.timer_sets += 1;
    }

    fn clear_timer(&self, id: EventLoopId) {
        self.state.lock().unwrap().timers.remove(&id);
    }

    fn keepalive(&self, id: EventLoopId, held: bool) {
        self.state.lock().unwrap().keepalive.insert(id, held);
    }
}

fn the_host() -> &'static Arc<ManualHost> {
    static HOST: OnceLock<Arc<ManualHost>> = OnceLock::new();
    HOST.get_or_init(|| {
        let h = Arc::new(ManualHost::default());
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
            let next = self.state.lock().unwrap().micro.pop_front();
            match next {
                Some(id) => {
                    drive_registered(id);
                    ran = true;
                }
                None => break,
            }
        }
        let next = self.state.lock().unwrap().macro_.pop_front();
        if let Some(id) = next {
            drive_registered(id);
            ran = true;
        }
        let due = {
            let mut s = self.state.lock().unwrap();
            let now = Instant::now();
            let due: Vec<EventLoopId> = s.timers.iter().filter(|(_, at)| **at <= now).map(|(id, _)| *id).collect();
            for id in &due {
                s.timers.remove(id);
            }
            due
        };
        for id in due {
            drive_registered(id);
            ran = true;
        }
        ran
    }

    /// Steps until `done()` or the deadline, sleeping until the next timer
    /// when nothing else is queued.
    fn run_until(&self, mut done: impl FnMut() -> bool, deadline: Duration) {
        let end = Instant::now() + deadline;
        while !done() {
            assert!(Instant::now() < end, "the host loop did not finish within {deadline:?}");
            if !self.step() {
                let next = self.state.lock().unwrap().timers.values().min().copied();
                match next {
                    Some(at) => std::thread::sleep(at.saturating_duration_since(Instant::now()).min(Duration::from_millis(20))),
                    None => std::thread::sleep(Duration::from_millis(1)),
                }
            }
        }
    }

    fn counts(&self) -> (usize, usize, usize) {
        let s = self.state.lock().unwrap();
        (s.scheduled_micro, s.scheduled_macro, s.timer_sets)
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
    let (_, _, timer_sets) = the_host().counts();
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
    the_host().run_until(|| the_host().state.lock().unwrap().micro.is_empty(), Duration::from_secs(1));
    let (micro_before, _, _) = the_host().counts();
    // The send comes from outside any drive (this thread, no runtime entered).
    tx.send(7).unwrap();
    let (micro_after, _, _) = the_host().counts();
    assert_eq!(micro_after - micro_before, 1, "exactly one microtask drive for the wake");
    the_host().run_until(|| got.load(Ordering::SeqCst) == 7, Duration::from_secs(1));
    drop(rt);
}

#[test]
fn many_wakes_between_drives_coalesce_into_one() {
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
    the_host().run_until(|| the_host().state.lock().unwrap().micro.is_empty(), Duration::from_secs(1));
    let (micro_before, _, _) = the_host().counts();
    for _ in 0..100 {
        notify.notify_one();
    }
    let (micro_after, _, _) = the_host().counts();
    assert_eq!(micro_after - micro_before, 1, "a hundred wakes, one scheduled drive");
    the_host().run_until(|| count.load(Ordering::SeqCst) >= 1, Duration::from_secs(1));
    drop(rt);
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
    let (_, macro_before, _) = the_host().counts();
    the_host().run_until(|| done.load(Ordering::SeqCst) == 1, Duration::from_secs(5));
    let (_, macro_after, _) = the_host().counts();
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
    the_host().run_until(|| the_host().state.lock().unwrap().micro.is_empty(), Duration::from_secs(1));
    let id = *the_host().state.lock().unwrap().keepalive.keys().last().unwrap();
    assert_eq!(the_host().state.lock().unwrap().keepalive[&id], true, "held while a task sleeps");
    the_host().run_until(|| done.load(Ordering::SeqCst) == 1, Duration::from_secs(2));
    // The completing drive released the hold.
    the_host().run_until(|| the_host().state.lock().unwrap().keepalive[&id] == false, Duration::from_secs(1));
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
