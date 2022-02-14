//! Generic clock defination.
//!
//! This module is most craeted for testing. You can easily test rate limit algorithm with `MockClock`.

use std::time::{Duration, SystemTime};

pub type Timestamp = u64;

pub trait Clock {
    fn now(&self) -> Timestamp;
}

/// `SystemClock` use `std::time::SystemTime` to get current timestamp. Use this clock in [`LeakyBucket`](crate::gcra::LeakyBucket) means
/// time passing is measure in real time.
///
/// # Example
/// ```no-run
/// let policy = LeakyBucket::with_clock(SystemClock);
/// ```
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Timestamp {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }
}

/// `MockClock` . Use this clock in [`LeakyBucket`](crate::gcra::LeakyBucket) means
/// time passing is measure in a user controled time.
///
/// # Example
/// ```no-run
/// let policy = LeakyBucket::with_clock(MockClock::new_now());
/// ```
pub struct MockClock(Timestamp);

impl MockClock {
    pub fn new_now() -> Self {
        Self::new(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        )
    }

    pub fn new(now: Timestamp) -> Self {
        MockClock(now)
    }

    pub fn forward(&mut self, dur: Duration) {
        self.0 += dur.as_millis() as u64;
    }

    pub fn backward(&mut self, dur: Duration) {
        self.0 -= dur.as_millis() as u64;
    }
}

impl Clock for MockClock {
    fn now(&self) -> Timestamp {
        self.0
    }
}
