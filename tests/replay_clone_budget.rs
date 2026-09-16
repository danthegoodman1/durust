//! Allocation budget for the cold replay path.
//!
//! Two copies live here, and both are invisible to every behavioural test — a
//! deep copy of an immutable event produces identical history — so they need a
//! guard that measures allocation rather than behaviour:
//!
//! * `match_or_append_command` borrows the recorded command event instead of
//!   cloning it.
//! * `split_start_event` moves the first recovery chunk's tail into the
//!   runtime instead of deep-cloning it out of the caller's `Vec`.
//!
//! The guard is differential, not an absolute ceiling. The same replay is run
//! twice with two inline payload sizes and only the *growth* is asserted, so
//! unrelated allocation churn elsewhere in the worker cancels out: it is
//! present in both arms. Each restored copy adds exactly one payload-sized
//! copy per replayed command event, which shows up as payload-proportional
//! growth and nothing else.
//!
//! This file is its own test binary, so the `#[global_allocator]` below
//! affects no other suite, and it holds exactly one `#[test]` so the counters
//! are never shared with a concurrently running test.

use durust::provider::{DurableBackend, HistoryEventData};
use durust::{Client, EventId, MemoryBackend, PayloadRef, PayloadStorageConfig, Worker};
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
///
/// Measured, not guessed, under the pinned single-chunk worker config below.
/// The middle rows are mutants of the *shipped* tree, which is what a guard
/// against re-introduction has to measure; genuine HEAD is given separately
/// because it is the honest baseline for what the two moves are worth:
///
/// | arm                                        | copies | allocations |
/// | ------------------------------------------ | ------ | ----------- |
/// | both copies removed (shipped)               |   4.75 |         483 |
/// | matcher `.cloned()` restored                |   5.75 |         550 |
/// | `split_start_event` deep clone restored     |   5.75 |         578 |
/// | both restored                               |   6.75 |         645 |
/// | genuine HEAD, for reference                 |   5.75 |         577 |
///
/// Each move is worth exactly one payload copy per replayed command event on
/// its own, and the pair is worth **1.00 copy and 94 allocations** against
/// genuine HEAD (5.75/577 to 4.75/483). The 6.75 in the both-restored row is a
/// property of that mutant rather than of HEAD, and no budget is set from it.
/// 5.25 sits midway between the shipped 4.75 and the 5.75 either single
/// regression produces, ~10% clear of both.
const MAX_PAYLOAD_COPIES_PER_COMMAND: f64 = 5.25;

/// Allocations for the small-payload cold replay. This is the fixed per-event
/// cost the two moves save that the payload ratio above cannot see, because it
/// is identical in both arms: 483 shipped, against 550 with the matcher clone
/// restored and 578 with the `split_start_event` clone restored. 515 sits
/// between the shipped number and the nearer of the two regressions.
const MAX_REPLAY_ALLOCATIONS: usize = 515;

/// How much the *count* of allocations may grow between the two payload
/// sizes. It is not zero and never was: the workflow re-serialises its own
/// activity inputs on every replay, and a `Vec<u8>` grown geometrically to
/// 64 KiB takes six more growth steps than one grown to 1 KiB, so a 64×
/// payload ratio buys 6 counted reallocations per command — 48 for the eight
/// commands here, measured identically in all four arms above. The budget is
/// this inherent term plus headroom; if it trips, an allocation *count* has
/// started tracking payload size, and the ratio above is no longer measuring
/// only copies.
const MAX_PAYLOAD_SIZE_ALLOCATION_GROWTH: usize = 64;

const PAYLOAD_COPY_BUDGET_RATIONALE: &str = "\
Every budget here was set by measuring the same replay with and without the \
two copies it guards: `peek_replay_command_event().cloned()` at the matcher, \
and `split_start_event` deep-cloning its chunk instead of moving it. If one \
trips, first check whether either has come back. If a copy or an allocation \
was added deliberately somewhere else, re-measure all four arms and move the \
budget with a note saying what the new cost is.";

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
        // Both arms must replay in the same number of chunks or the
        // differential measures chunking, not payload copies. The default
        // byte budget is 256 KiB, which the large arm's eight 64 KiB inputs
        // overrun, so it used to cold-replay in several chunks while the
        // small arm replayed in one — a structural difference between the
        // arms that has nothing to do with the matcher. Both bounds are
        // raised past the whole history so the only difference left between
        // the arms is the size of the payloads themselves.
        .history_chunk_events(4_096)
        .history_chunk_bytes(8 * 1024 * 1024)
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
            .stream_history_for_replay(durust::provider::StreamHistoryRequest {
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

    // Allocation count must stay within its inherent payload-size term; if it
    // starts scaling beyond that, the budget above is measuring the wrong
    // thing.
    assert!(
        large.allocations <= small.allocations + MAX_PAYLOAD_SIZE_ALLOCATION_GROWTH,
        "cold replay allocated {} times at {LARGE_PAYLOAD_BYTES} byte payloads against {} times \
         at {SMALL_PAYLOAD_BYTES} bytes, a growth of {} over the budget of \
         {MAX_PAYLOAD_SIZE_ALLOCATION_GROWTH}; allocation count must not scale with payload size \
         beyond the workflow's own re-serialisation.\n{PAYLOAD_COPY_BUDGET_RATIONALE}",
        large.allocations,
        small.allocations,
        large.allocations - small.allocations,
    );
}
