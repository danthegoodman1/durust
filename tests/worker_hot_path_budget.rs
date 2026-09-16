//! Allocation budgets for the worker's per-task hot path.
//!
//! Three copies the worker used to make per task are gone, and none of them is
//! visible to a behavioural test — a deep copy of an immutable value produces
//! identical history — so each needs a guard that measures allocation rather
//! than behaviour:
//!
//! * the batched commit loop **moves** each `WorkflowTaskCommit` into the batch
//!   instead of cloning it and dropping the original unread;
//! * `prefetched_claim_history_chunk` **validates before cloning**, so a
//!   prefetch that does not cover the requested window costs a scan rather
//!   than a deep copy of every candidate event it then discards;
//! * the run's change markers are carried as one shared, deduplicated index
//!   instead of a record vector cloned and re-indexed on every task.
//!
//! The first two are asserted differentially: the same work is done twice with
//! two inline payload sizes and only the *growth* is asserted, so unrelated
//! allocation churn cancels out because it is present in both arms. The third
//! is asserted as a **slope** — allocations per extra change marker per cached
//! task — because what it removes is per-record allocation, not payload bytes.
//!
//! This file is its own test binary, so the `#[global_allocator]` below
//! affects no other suite, and it holds exactly one `#[test]` so the counters
//! are never shared with a concurrently running test.

use durust::{Client, DurableBranchExt, MemoryBackend, PayloadStorageConfig, Worker};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

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

/// Above this the backend stores payloads as blob refs and every
/// payload-proportional measurement here collapses to zero for the wrong
/// reason.
const INLINE_THRESHOLD_BYTES: usize = 1024 * 1024;
const SMALL_PAYLOAD_BYTES: usize = 1024;
const LARGE_PAYLOAD_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Sizing {
    payload_bytes: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BulkInput {
    bytes: Vec<u8>,
}

#[durust::activity(name = "hot-path-budget.echo")]
async fn echo(input: BulkInput) -> durust::Result<BulkInput> {
    Ok(input)
}

// ---------------------------------------------------------------------------
// Batched commit
// ---------------------------------------------------------------------------

/// One activity carrying an inline payload, then done. The first workflow task
/// puts that payload into the commit twice — once in the appended
/// `ActivityScheduled` event and once in the scheduled activity task — so
/// cloning the commit copies it twice per task.
#[durust::workflow(name = "hot-path-budget.one-activity", version = 1)]
async fn one_activity_workflow(input: Sizing) -> durust::Result<usize> {
    let out = durust::call_activity!(echo(BulkInput {
        bytes: vec![7u8; input.payload_bytes],
    }))
    .task_queue("activities")
    .await?;
    Ok(out.bytes.len())
}

/// Runs in the batched commit path: `BATCHED_TASKS` claims taken in one RPC and
/// committed in one batch, which is what `run_pass_once` does in production.
const BATCHED_TASKS: usize = 8;

fn batch_worker(backend: MemoryBackend) -> Worker<MemoryBackend> {
    Worker::builder(backend)
        .worker_id("hot-path-budget-batch")
        .workflow_task_queue("workflows")
        .activity_task_queue("activities")
        .register_workflow(one_activity_workflow)
        .register_activity(echo)
        .max_concurrent_workflow_tasks(BATCHED_TASKS)
        .workflow_task_prefetch_limit(BATCHED_TASKS)
        .workflow_task_commit_batch_size(BATCHED_TASKS)
        .build()
}

fn measure_batched_commit(payload_bytes: usize) -> Cost {
    block_on(async {
        let backend = MemoryBackend::with_payload_storage(
            PayloadStorageConfig::new().inline_threshold_bytes(INLINE_THRESHOLD_BYTES),
        );
        let client = Client::new(backend.clone());
        for index in 0..BATCHED_TASKS {
            client
                .start_workflow::<one_activity_workflow>(
                    format!("hot-path-budget/batch-{index}"),
                    "workflows",
                    Sizing { payload_bytes },
                )
                .await
                .unwrap();
        }
        // Built before recording so registry and builder allocations do not
        // land in the measured region.
        let mut worker = batch_worker(backend.clone());

        record(|| async {
            let committed = worker.run_workflow_batch_once().await.unwrap();
            assert_eq!(
                committed, BATCHED_TASKS,
                "every started run must commit in the one batch"
            );
        })
        .await
    })
}

// ---------------------------------------------------------------------------
// Rejected prefetch
// ---------------------------------------------------------------------------

/// More than the sixteen events `MemoryBackend` prefetches onto a claim, so the
/// cached wake's requested window starts before the prefetch does and the
/// prefetch is rejected. That is the path that used to deep-copy all sixteen
/// candidate events — payloads included — before discovering it could not use
/// any of them.
const FANOUT: usize = 24;

#[durust::workflow(name = "hot-path-budget.fanout", version = 1)]
async fn fanout_workflow(input: Sizing) -> durust::Result<usize> {
    let mut handles = Vec::new();
    for _ in 0..FANOUT {
        handles.push(
            durust::call_activity!(echo(BulkInput {
                bytes: vec![3u8; input.payload_bytes],
            }))
            .task_queue("activities")
            .spawn()
            .await?,
        );
    }
    let mut total = 0;
    for handle in handles {
        total += handle.result().await?.bytes.len();
    }
    Ok(total)
}

fn fanout_worker(backend: MemoryBackend) -> Worker<MemoryBackend> {
    Worker::builder(backend)
        .worker_id("hot-path-budget-fanout")
        .workflow_task_queue("workflows")
        .activity_task_queue("activities")
        .register_workflow(fanout_workflow)
        .register_activity(echo)
        // One chunk for the whole window, so the two payload arms differ only
        // in payload size and not in how many chunks they stream.
        .history_chunk_events(4_096)
        .history_chunk_bytes(64 * 1024 * 1024)
        .build()
}

fn measure_rejected_prefetch(payload_bytes: usize) -> Cost {
    block_on(async {
        let backend = MemoryBackend::with_payload_storage(
            PayloadStorageConfig::new().inline_threshold_bytes(INLINE_THRESHOLD_BYTES),
        );
        let client = Client::new(backend.clone());
        client
            .start_workflow::<fanout_workflow>(
                "hot-path-budget/fanout",
                "workflows",
                Sizing { payload_bytes },
            )
            .await
            .unwrap();

        let mut worker = fanout_worker(backend.clone());
        // Task one schedules the whole fanout and parks. The run stays cached.
        assert!(worker.run_workflow_once().await.unwrap());
        assert_eq!(worker.cached_workflow_count(), 1);
        let mut completed = 0;
        loop {
            let batch = worker.run_activity_batch_once().await.unwrap();
            if batch == 0 {
                break;
            }
            completed += batch;
        }
        assert_eq!(completed, FANOUT);

        // The cached wake now asks for a window `FANOUT` events wide against a
        // claim carrying only the last sixteen, so the prefetch cannot serve
        // it and the chunk is streamed instead.
        record(|| async {
            assert!(worker.run_workflow_once().await.unwrap());
        })
        .await
    })
}

// ---------------------------------------------------------------------------
// Cached change markers
// ---------------------------------------------------------------------------

/// Cached tasks measured after the markers are recorded. The first task
/// appends the markers and is deliberately not cached (an appended change
/// marker forces a cold replay), the second cold-replays them out of history,
/// and every task after that is the steady state this measures.
const MARKER_TASKS: usize = 6;
const FEW_MARKERS: usize = 2;
const MANY_MARKERS: usize = 18;

#[durust::activity(name = "hot-path-budget.tick")]
async fn tick(_input: UnitInput) -> durust::Result<u64> {
    Ok(1)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct UnitInput {}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct MarkerInput {
    markers: usize,
    rounds: usize,
}

/// Records `markers` change markers once, then takes one workflow task per
/// round. The markers are never read again — a change marker is decided once
/// per run, and re-deciding one across tasks is nondeterminism, not a hot
/// path — so what every later task carries is purely the recorded set, and
/// what the shared index removes is the per-task rebuild of it.
#[durust::workflow(name = "hot-path-budget.markers", version = 1)]
async fn marker_workflow(input: MarkerInput) -> durust::Result<u64> {
    for index in 0..input.markers {
        let _ = durust::get_version(format!("change-{index}"), 1, 2).await?;
    }
    let mut total = 0;
    for _ in 0..input.rounds {
        total += durust::call_activity!(tick(UnitInput {}))
            .task_queue("activities")
            .await?;
    }
    Ok(total)
}

fn marker_worker(backend: MemoryBackend) -> Worker<MemoryBackend> {
    Worker::builder(backend)
        .worker_id("hot-path-budget-markers")
        .workflow_task_queue("workflows")
        .activity_task_queue("activities")
        .register_workflow(marker_workflow)
        .register_activity(tick)
        .build()
}

/// Allocations for `MARKER_TASKS` *cached* workflow tasks of a run holding
/// `markers` recorded change markers.
fn measure_cached_marker_tasks(markers: usize) -> Cost {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        client
            .start_workflow::<marker_workflow>(
                "hot-path-budget/markers",
                "workflows",
                MarkerInput {
                    markers,
                    rounds: MARKER_TASKS + 4,
                },
            )
            .await
            .unwrap();
        let mut worker = marker_worker(backend.clone());

        // Two warm-up rounds: the first appends the markers and is not cached,
        // the second cold-replays them and is. Everything measured below runs
        // against a cached run whose marker set is already complete.
        for _ in 0..2 {
            assert!(worker.run_workflow_once().await.unwrap());
            assert_eq!(worker.run_activity_batch_once().await.unwrap(), 1);
        }
        assert_eq!(
            worker.cached_workflow_count(),
            1,
            "the marker run must be cached before the measured tasks"
        );

        record(|| async {
            for _ in 0..MARKER_TASKS {
                assert!(worker.run_workflow_once().await.unwrap());
                assert_eq!(worker.run_activity_batch_once().await.unwrap(), 1);
            }
        })
        .await
    })
}

// ---------------------------------------------------------------------------
// Budgets
// ---------------------------------------------------------------------------

/// Payload copies one batched, committed workflow task may cost.
///
/// Measured: moving the commit into the batch costs **4.00** copies per task
/// and cloning it costs **6.00**. The difference is exactly the two copies one
/// commit holds — the payload reaches the provider once in the appended
/// `ActivityScheduled` event and once in the scheduled activity task — which
/// is what says the delta is the commit clone and not something else. 5.00
/// sits midway.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct WaiterInput {
    waiters: usize,
    rounds: usize,
}

// `waiters` signal waits that never resolve, joined with `rounds` spawned
// activities: every completion wakes the cached run, and the join polls each
// pending waiter once per wake, which is the per-waiter cost being measured.
#[durust::workflow(name = "hot-path-budget.waiters", version = 1)]
async fn waiter_workflow(input: WaiterInput) -> durust::Result<u64> {
    let mut branches: Vec<durust::BoxSelectBranch<u64>> = Vec::new();
    for index in 0..input.waiters {
        branches.push(durust::signal::<u64>(format!("never-{index}")).boxed());
    }
    for _ in 0..input.rounds {
        let handle = durust::call_activity!(tick(UnitInput {}))
            .task_queue("activities")
            .spawn()
            .await?;
        branches.push(handle.result().boxed());
    }
    let values = durust::join_all(branches).await?;
    Ok(values.into_iter().sum())
}

fn waiter_worker(backend: MemoryBackend) -> Worker<MemoryBackend> {
    Worker::builder(backend)
        .worker_id("hot-path-budget-waiters")
        .workflow_task_queue("workflows")
        .activity_task_queue("activities")
        .register_workflow(waiter_workflow)
        .register_activity(tick)
        .build()
}

const WAITER_ROUNDS: usize = 6;
const FEW_WAITERS: usize = 2;
const MANY_WAITERS: usize = 10;

fn measure_cached_signal_waiter_wakes(waiters: usize) -> Cost {
    block_on(async {
        let backend = MemoryBackend::new();
        let client = Client::new(backend.clone());
        client
            .start_workflow::<waiter_workflow>(
                "hot-path-budget/waiters",
                "workflows",
                WaiterInput {
                    waiters,
                    rounds: WAITER_ROUNDS + 1,
                },
            )
            .await
            .unwrap();
        let mut worker = waiter_worker(backend.clone());
        assert!(worker.run_workflow_once().await.unwrap());
        // One completion primes the cache so the measured wakes are all hot.
        assert!(worker.run_activity_once().await.unwrap());
        assert!(worker.run_workflow_once().await.unwrap());
        assert_eq!(worker.cached_workflow_count(), 1);

        record(|| async {
            for _ in 0..WAITER_ROUNDS {
                assert!(worker.run_activity_once().await.unwrap());
                assert!(worker.run_workflow_once().await.unwrap());
            }
        })
        .await
    })
}

const MAX_COMMIT_PAYLOAD_COPIES_PER_TASK: f64 = 5.0;

/// Payload copies per completed activity one cached wake may cost when its
/// claim's prefetch cannot serve the window.
///
/// Measured: validating first costs **3.67** copies per completed activity —
/// the streamed chunk plus the run's own re-execution — and cloning the
/// prefetched candidates before rejecting them costs **4.33**. The difference
/// is 0.667 per activity over `FANOUT` = 24, which is exactly the sixteen
/// events `MemoryBackend` prefetches onto a claim and this path then threw
/// away. 4.00 sits midway.
const MAX_REJECTED_PREFETCH_PAYLOAD_COPIES: f64 = 4.0;

/// Allocations one extra recorded change marker may add to one cached
/// workflow task.
///
/// This one is a slope rather than a level, because the level is dominated by
/// per-task costs that have nothing to do with the marker count. Measured:
/// the shared index costs **0.5** allocations per marker per cached task
/// (914 and 958 allocations, exactly reproducible across runs), and against
/// genuine HEAD — the record vector cloned in, re-indexed, cloned again for
/// the context, and cloned once more into the cache entry, at six owned
/// strings per record — **18.7**.
///
/// 3.0 is six times the shipped slope, but the margin against a regression is
/// not uniform and it is worth knowing which way. A proxy that copies only
/// the marker index *twice* per task reads 6.7, caught by better than two
/// times. A proxy that copies it *once* reads 3.6, caught by 20%. So the
/// budget's real guarantee is "one extra copy of the marker index per task is
/// caught"; anything cheaper than that would not be.
const MAX_ALLOCATIONS_PER_MARKER_PER_TASK: f64 = 3.0;

/// Allocations a pending signal waiter may add to each cached wake it is
/// polled through. Measured at 8.2 while the waiting poll cloned its command
/// id and name and built a fingerprint before it knew whether anything had
/// arrived, and 4.2 once those went. What remains is the inbox read each
/// waiter still makes on every wake: the request's owned name and run id and
/// the provider's answer. Gating those reads on the claim reason measured
/// under 1 but changes the commit for a signal that arrived before an
/// activity completion, which TypeScript consumes in that task.
const MAX_ALLOCATIONS_PER_SIGNAL_WAITER_PER_WAKE: f64 = 5.0;

const BUDGET_RATIONALE: &str = "\
Every budget in this file was set by measuring the same work with and without \
the copy it guards. If one trips, first check whether that copy has come \
back: the batch commit cloning `task.commit`, `prefetched_claim_history_chunk` \
collecting cloned events before validating them, or the change-marker index \
being rebuilt per task instead of carried. If a copy or an allocation was \
added deliberately somewhere else, re-measure both arms and move the budget \
with a note saying what the new cost is.";

struct Cost {
    bytes: usize,
    allocations: usize,
}

async fn record<F, Fut>(body: F) -> Cost
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = ()>,
{
    ALLOCATED_BYTES.store(0, Ordering::Relaxed);
    ALLOCATIONS.store(0, Ordering::Relaxed);
    RECORDING.store(true, Ordering::Relaxed);
    body().await;
    RECORDING.store(false, Ordering::Relaxed);
    Cost {
        bytes: ALLOCATED_BYTES.load(Ordering::Relaxed),
        allocations: ALLOCATIONS.load(Ordering::Relaxed),
    }
}

fn payload_copies(small: &Cost, large: &Cost, units: usize) -> f64 {
    let growth = large.bytes.saturating_sub(small.bytes) as f64;
    let payload_delta = (LARGE_PAYLOAD_BYTES - SMALL_PAYLOAD_BYTES) as f64;
    growth / (payload_delta * units as f64)
}

#[test]
fn worker_hot_path_holds_its_per_task_allocation_budgets() {
    let commit_small = measure_batched_commit(SMALL_PAYLOAD_BYTES);
    let commit_large = measure_batched_commit(LARGE_PAYLOAD_BYTES);
    let commit_copies = payload_copies(&commit_small, &commit_large, BATCHED_TASKS);
    assert!(
        commit_copies < MAX_COMMIT_PAYLOAD_COPIES_PER_TASK,
        "a batch of {BATCHED_TASKS} committed workflow tasks copied each task's payload \
         {commit_copies:.2} times, over the budget of {MAX_COMMIT_PAYLOAD_COPIES_PER_TASK}.\n\
         small arm: {} bytes in {} allocations at {SMALL_PAYLOAD_BYTES} byte payloads\n\
         large arm: {} bytes in {} allocations at {LARGE_PAYLOAD_BYTES} byte payloads\n\
         {BUDGET_RATIONALE}",
        commit_small.bytes,
        commit_small.allocations,
        commit_large.bytes,
        commit_large.allocations,
    );

    let prefetch_small = measure_rejected_prefetch(SMALL_PAYLOAD_BYTES);
    let prefetch_large = measure_rejected_prefetch(LARGE_PAYLOAD_BYTES);
    let prefetch_copies = payload_copies(&prefetch_small, &prefetch_large, FANOUT);
    assert!(
        prefetch_copies < MAX_REJECTED_PREFETCH_PAYLOAD_COPIES,
        "a cached wake over {FANOUT} completions whose prefetch could not serve the window \
         copied each payload {prefetch_copies:.2} times, over the budget of \
         {MAX_REJECTED_PREFETCH_PAYLOAD_COPIES}.\n\
         small arm: {} bytes in {} allocations at {SMALL_PAYLOAD_BYTES} byte payloads\n\
         large arm: {} bytes in {} allocations at {LARGE_PAYLOAD_BYTES} byte payloads\n\
         {BUDGET_RATIONALE}",
        prefetch_small.bytes,
        prefetch_small.allocations,
        prefetch_large.bytes,
        prefetch_large.allocations,
    );

    let few = measure_cached_marker_tasks(FEW_MARKERS);
    let many = measure_cached_marker_tasks(MANY_MARKERS);
    let per_marker_per_task = (many.allocations.saturating_sub(few.allocations)) as f64
        / ((MANY_MARKERS - FEW_MARKERS) * MARKER_TASKS) as f64;
    assert!(
        per_marker_per_task < MAX_ALLOCATIONS_PER_MARKER_PER_TASK,
        "each recorded change marker cost {per_marker_per_task:.1} allocations per cached \
         workflow task, over the budget of {MAX_ALLOCATIONS_PER_MARKER_PER_TASK}.\n\
         {FEW_MARKERS} markers: {} allocations over {MARKER_TASKS} cached tasks\n\
         {MANY_MARKERS} markers: {} allocations over {MARKER_TASKS} cached tasks\n\
         {BUDGET_RATIONALE}",
        few.allocations,
        many.allocations,
    );

    let few_waiters = measure_cached_signal_waiter_wakes(FEW_WAITERS);
    let many_waiters = measure_cached_signal_waiter_wakes(MANY_WAITERS);
    let per_waiter_per_wake = (many_waiters
        .allocations
        .saturating_sub(few_waiters.allocations)) as f64
        / ((MANY_WAITERS - FEW_WAITERS) * WAITER_ROUNDS) as f64;
    assert!(
        per_waiter_per_wake < MAX_ALLOCATIONS_PER_SIGNAL_WAITER_PER_WAKE,
        "each pending signal waiter cost {per_waiter_per_wake:.1} allocations per cached wake, \
         over the budget of {MAX_ALLOCATIONS_PER_SIGNAL_WAITER_PER_WAKE}.\n\
         {FEW_WAITERS} waiters: {} allocations over {WAITER_ROUNDS} wakes\n\
         {MANY_WAITERS} waiters: {} allocations over {WAITER_ROUNDS} wakes\n\
         {BUDGET_RATIONALE}",
        few_waiters.allocations,
        many_waiters.allocations,
    );
}
