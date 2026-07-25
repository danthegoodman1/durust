//! Allocation budget for the replay command matcher.
//!
//! `match_or_append_command` borrows the recorded command event instead of
//! cloning it. That is invisible to every behavioural test — a deep copy of an
//! immutable event produces identical history — so it needs a guard that
//! measures allocation rather than behaviour.
//!
//! The guard is differential, not an absolute ceiling. The same replay is run
//! twice with two inline payload sizes and only the *growth* is asserted, so
//! unrelated allocation churn elsewhere in the worker cancels out: it is
//! present in both arms. What a reintroduced `peek_replay_command_event()
//! .cloned()` adds is exactly one payload-sized copy per replayed command
//! event, which shows up as payload-proportional growth and nothing else.
//!
//! This file is its own test binary, so the `#[global_allocator]` below
//! affects no other suite, and it holds exactly one `#[test]` so the counters
//! are never shared with a concurrently running test.

use durust::{
    Client, DurableBackend, EventId, HistoryEventData, MemoryBackend, PayloadRef,
    PayloadStorageConfig, Worker,
};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

static RECORDING: AtomicBool = AtomicBool::new(false);
static ALLOCATED_BYTES: AtomicUsize = AtomicUsize::new(0);
static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

struct CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if RECORDING.load(Ordering::Relaxed) {
            ALLOCATED_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if RECORDING.load(Ordering::Relaxed) && new_size > layout.size() {
            ALLOCATED_BYTES.fetch_add(new_size - layout.size(), Ordering::Relaxed);
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

/// Command events in the replayed history. Each one is a separate chance for
/// the matcher to copy a payload.
const COMMANDS: u64 = 8;
const SMALL_PAYLOAD_BYTES: usize = 1024;
const LARGE_PAYLOAD_BYTES: usize = 64 * 1024;
/// Above `DEFAULT_INLINE_THRESHOLD_BYTES` (8 KiB), the backend would store the
/// large arm as a blob ref and the whole measurement would collapse to zero
/// growth for the wrong reason.
const INLINE_THRESHOLD_BYTES: usize = 1024 * 1024;

/// How many payload-sized copies one replayed command event may cost.
/// Measured, not guessed: the clone-free matcher lands on 5.10 and restoring
/// `peek_replay_command_event().cloned()` lands on 6.10 — exactly one more
/// copy per command event, as it should. 5.5 sits midway, ~8% clear of both.
const MAX_PAYLOAD_COPIES_PER_COMMAND: f64 = 5.5;

/// Allocations for the small-payload cold replay. This is the fixed per-event
/// cost the borrow saves and the payload ratio above cannot see, because it is
/// identical in both arms: 577 clone-free against 644 with the clone restored,
/// about eight allocations per replayed command event for the event's run id,
/// type name, task queue, retry policy and fingerprint strings.
const MAX_REPLAY_ALLOCATIONS: usize = 610;

const PAYLOAD_COPY_BUDGET_RATIONALE: &str = "\
Both budgets were set by measuring the same replay with and without \
`peek_replay_command_event().cloned()` at the matcher. If one trips, first \
check whether a command event is being cloned again. If a copy or an \
allocation was added deliberately somewhere else, re-measure both arms and \
move the budget with a note saying what the new cost is.";

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Sizing {
    payload_bytes: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BulkInput {
    bytes: Vec<u8>,
}

#[durust::activity(name = "clone-budget.bulk")]
async fn bulk(input: BulkInput) -> durust::Result<usize> {
    Ok(input.bytes.len())
}

/// Schedules `COMMANDS` activities carrying an inline payload, then sleeps, so
/// the following workflow task has to match every recorded `ActivityScheduled`
/// plus a `TimerStarted` before it can drain the completions.
#[durust::workflow(name = "clone-budget.commands", version = 1)]
async fn command_replay_workflow(input: Sizing) -> durust::Result<usize> {
    let mut handles = Vec::new();
    for index in 0..COMMANDS {
        let handle = durust::call_activity!(bulk(BulkInput {
            bytes: vec![index as u8; input.payload_bytes],
        }))
        .task_queue("activities")
        .spawn()
        .await?;
        handles.push(handle);
    }
    durust::sleep(Duration::ZERO).await?;
    let mut total = 0;
    for handle in handles {
        total += handle.result().await?;
    }
    Ok(total)
}

fn worker(backend: MemoryBackend) -> Worker<MemoryBackend> {
    Worker::builder(backend)
        .workflow_task_queue("workflows")
        .activity_task_queue("activities")
        .register_workflow(command_replay_workflow)
        .register_activity(bulk)
        .build()
}

struct ReplayCost {
    bytes: usize,
    allocations: usize,
}

/// Drives a run up to the replay point, then measures allocations for a single
/// cold workflow task that matches every recorded command event and appends
/// none.
fn measure_cold_replay(payload_bytes: usize) -> ReplayCost {
    block_on(async {
        let backend = MemoryBackend::with_payload_storage(
            PayloadStorageConfig::new().inline_threshold_bytes(INLINE_THRESHOLD_BYTES),
        );
        let client = Client::new(backend.clone());
        let run_id = client
            .start_workflow::<command_replay_workflow>(
                "clone-budget/run",
                "workflows",
                Sizing { payload_bytes },
            )
            .await
            .unwrap();

        let mut setup = worker(backend.clone());
        assert!(setup.run_workflow_once().await.unwrap());
        let mut completed = 0;
        loop {
            let batch = setup.run_activity_batch_once().await.unwrap();
            if batch == 0 {
                break;
            }
            completed += batch;
        }
        assert_eq!(completed, COMMANDS as usize);
        assert_eq!(setup.run_timers_once().await.unwrap(), 1);
        drop(setup);

        // `stream_history_for_replay`, not `stream_history`: the latter hydrates
        // blob refs back to inline, so it reports "inline" whatever the backend
        // stored and this check would pass while measuring nothing. That is the
        // exact trap this file exists to guard against.
        let recorded = backend
            .stream_history_for_replay(durust::StreamHistoryRequest {
                run_id: run_id.clone(),
                after_event_id: EventId::ZERO,
                up_to_event_id: EventId(1_000_000),
                max_events: 1_000,
                max_bytes: usize::MAX,
            })
            .await
            .unwrap()
            .events;
        let scheduled = recorded
            .iter()
            .filter_map(|event| match &event.data {
                HistoryEventData::ActivityScheduled(scheduled) => Some(&scheduled.input),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(scheduled.len(), COMMANDS as usize);
        for input in &scheduled {
            let PayloadRef::Inline { bytes, .. } = input else {
                panic!("the recorded activity input must stay inline for this measurement");
            };
            assert!(bytes.len() >= payload_bytes);
        }

        // Build the worker before recording so registry and builder
        // allocations do not land in the measured region.
        let mut replay = worker(backend.clone());

        ALLOCATED_BYTES.store(0, Ordering::Relaxed);
        ALLOCATIONS.store(0, Ordering::Relaxed);
        RECORDING.store(true, Ordering::Relaxed);
        let ran = replay.run_workflow_once().await.unwrap();
        RECORDING.store(false, Ordering::Relaxed);
        assert!(ran, "the cold replay task must run");

        ReplayCost {
            bytes: ALLOCATED_BYTES.load(Ordering::Relaxed),
            allocations: ALLOCATIONS.load(Ordering::Relaxed),
        }
    })
}

#[test]
fn replaying_a_command_event_does_not_copy_its_payload() {
    let small = measure_cold_replay(SMALL_PAYLOAD_BYTES);
    let large = measure_cold_replay(LARGE_PAYLOAD_BYTES);

    let payload_growth = large.bytes.saturating_sub(small.bytes) as f64;
    let payload_delta = (LARGE_PAYLOAD_BYTES - SMALL_PAYLOAD_BYTES) as f64;
    let copies_per_command = payload_growth / (payload_delta * COMMANDS as f64);

    assert!(
        copies_per_command < MAX_PAYLOAD_COPIES_PER_COMMAND,
        "cold replay of {COMMANDS} command events copied their payloads \
         {copies_per_command:.2} times each, over the budget of \
         {MAX_PAYLOAD_COPIES_PER_COMMAND}.\n\
         small arm: {} bytes in {} allocations at {SMALL_PAYLOAD_BYTES} byte payloads\n\
         large arm: {} bytes in {} allocations at {LARGE_PAYLOAD_BYTES} byte payloads\n\
         {PAYLOAD_COPY_BUDGET_RATIONALE}",
        small.bytes,
        small.allocations,
        large.bytes,
        large.allocations,
    );

    // Payload-proportional growth is only half of what the borrow saves. The
    // other half is a fixed per-event allocation cost that is identical in
    // both arms and so invisible to the ratio above.
    assert!(
        small.allocations <= MAX_REPLAY_ALLOCATIONS,
        "cold replay of {COMMANDS} command events made {} allocations, over the budget of \
         {MAX_REPLAY_ALLOCATIONS}.\n{PAYLOAD_COPY_BUDGET_RATIONALE}",
        small.allocations,
    );

    // Allocation count is payload-size independent; if it starts scaling, the
    // budget above is measuring the wrong thing.
    assert!(
        large.allocations <= small.allocations + COMMANDS as usize,
        "cold replay allocated {} times at {LARGE_PAYLOAD_BYTES} byte payloads against {} times \
         at {SMALL_PAYLOAD_BYTES} bytes; allocation count must not scale with payload size",
        large.allocations,
        small.allocations,
    );
}
