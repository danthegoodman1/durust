use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VirtualInstant {
    millis: u64,
}

impl VirtualInstant {
    pub fn as_millis(self) -> u64 {
        self.millis
    }
}

#[derive(Clone, Debug, Default)]
pub struct VirtualClock {
    now: VirtualInstant,
}

impl VirtualClock {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn now(&self) -> VirtualInstant {
        self.now
    }

    pub fn advance(&mut self, duration: Duration) -> VirtualInstant {
        let millis = duration.as_millis().try_into().unwrap_or(u64::MAX);
        self.now.millis = self.now.millis.saturating_add(millis);
        self.now
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SimTaskId(u64);

impl SimTaskId {
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SimStep {
    pub task_id: SimTaskId,
    pub label: String,
}

#[derive(Clone, Debug)]
pub struct SeededScheduler {
    rng: XorShift64,
    next_task_id: u64,
    ready: VecDeque<SimStep>,
    delayed: BTreeMap<VirtualInstant, Vec<SimStep>>,
}

impl SeededScheduler {
    pub fn new(seed: u64) -> Self {
        Self {
            rng: XorShift64::new(seed),
            next_task_id: 0,
            ready: VecDeque::new(),
            delayed: BTreeMap::new(),
        }
    }

    pub fn spawn(&mut self, label: impl Into<String>) -> SimTaskId {
        self.next_task_id += 1;
        let task_id = SimTaskId(self.next_task_id);
        self.ready.push_back(SimStep {
            task_id,
            label: label.into(),
        });
        task_id
    }

    pub fn spawn_at(&mut self, at: VirtualInstant, label: impl Into<String>) -> SimTaskId {
        self.next_task_id += 1;
        let task_id = SimTaskId(self.next_task_id);
        self.delayed.entry(at).or_default().push(SimStep {
            task_id,
            label: label.into(),
        });
        task_id
    }

    pub fn wake_due(&mut self, now: VirtualInstant) {
        let due = self
            .delayed
            .keys()
            .copied()
            .take_while(|instant| instant <= &now)
            .collect::<Vec<_>>();
        for instant in due {
            if let Some(mut steps) = self.delayed.remove(&instant) {
                steps.sort_by_key(|step| step.task_id);
                self.ready.extend(steps);
            }
        }
    }

    pub fn next_step(&mut self) -> Option<SimStep> {
        if self.ready.is_empty() {
            return None;
        }
        let index = (self.rng.next() as usize) % self.ready.len();
        self.ready.remove(index)
    }
}

#[derive(Clone, Debug)]
struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    fn new(seed: u64) -> Self {
        Self { state: seed.max(1) }
    }

    fn next(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_produces_same_schedule() {
        fn trace(seed: u64) -> Vec<String> {
            let mut scheduler = SeededScheduler::new(seed);
            scheduler.spawn("a");
            scheduler.spawn("b");
            scheduler.spawn("c");
            std::iter::from_fn(|| scheduler.next_step().map(|step| step.label)).collect()
        }

        assert_eq!(trace(123), trace(123));
        assert_ne!(trace(123), trace(456));
    }

    #[test]
    fn virtual_clock_wakes_due_steps_in_stable_order() {
        let mut clock = VirtualClock::new();
        let mut scheduler = SeededScheduler::new(7);
        let later = clock.advance(Duration::from_millis(10));
        scheduler.spawn_at(later, "second");
        scheduler.spawn_at(later, "third");
        scheduler.spawn_at(VirtualInstant { millis: 5 }, "first");

        scheduler.wake_due(VirtualInstant { millis: 5 });
        assert_eq!(scheduler.next_step().unwrap().label, "first");
        assert!(scheduler.next_step().is_none());

        scheduler.wake_due(later);
        let labels =
            std::iter::from_fn(|| scheduler.next_step().map(|step| step.label)).collect::<Vec<_>>();
        assert_eq!(labels, vec!["second", "third"]);
    }
}
