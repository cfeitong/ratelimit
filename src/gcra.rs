//! Implementation of generic cell rate algorithm(https://en.wikipedia.org/wiki/Generic_cell_rate_algorithm)

use std::{sync::atomic::AtomicU64, time::Duration};

use parking_lot::Mutex;

use crate::clock::{Clock, MockClock, SystemClock};

pub trait Policy {
    fn pass(&self) -> bool;
}

pub struct LeakyBucket<C = SystemClock> {
    clock: C,
    state: Mutex<State>,
    burst: u64,
    rate: u64,
}

struct State {
    level: u64,
    lct: u64,
}

impl LeakyBucket<SystemClock> {
    pub fn new() -> Self {
        Self::builder().build()
    }
}

impl LeakyBucket<SystemClock> {
    pub fn builder() -> LeakyBucketBuilder<SystemClock> {
        LeakyBucketBuilder {
            clock: SystemClock,
            burst: 0,
            rate: 0,
        }
    }
}

pub struct LeakyBucketBuilder<C> {
    clock: C,
    burst: u64,
    rate: u64,
}

impl<C> LeakyBucketBuilder<C> {
    pub fn clock<NC>(self, clock: NC) -> LeakyBucketBuilder<NC> {
        LeakyBucketBuilder {
            clock,
            burst: self.burst,
            rate: self.rate,
        }
    }

    pub fn burst(mut self, extra_qps: u64) -> LeakyBucketBuilder<C> {
        self.burst = extra_qps;
        self
    }

    pub fn rate(mut self, qps: u64) -> LeakyBucketBuilder<C> {
        self.rate = qps;
        self
    }

    pub fn build(self) -> LeakyBucket<C> {
        LeakyBucket {
            clock: self.clock,
            state: Mutex::new(State {
                level: self.burst,
                lct: 0,
            }),
            burst: self.burst,
            rate: self.rate,
        }
    }
}

impl<C> Policy for LeakyBucket<C>
where
    C: Clock,
{
    fn pass(&self) -> bool {
        let mut state = self.state.lock();
        let now = self.clock.now();
        let dur_in_ms = now - state.lct;
        let leaked = dur_in_ms * self.rate / 1000;
        let new_level = state.level as i64 - leaked as i64;
        if new_level >= (self.rate + self.burst) as i64 {
            false
        } else {
            state.level = std::cmp::max(0, new_level) as u64 + 1;
            state.lct = now;
            true
        }
    }
}

impl LeakyBucket<MockClock> {
    pub fn forward(&mut self, dur: Duration) {
        self.clock.forward(dur);
    }
}

impl<C> LeakyBucket<C>
where
    C: Clock,
{
    fn decorate<'a, Req, Resp>(
        &'a self,
        mut f: impl FnMut(Req) -> Resp + 'a,
    ) -> impl FnMut(Req) -> Result<Resp, Req> + 'a {
        move |req| {
            if self.pass() {
                Ok(f(req))
            } else {
                Err(req)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::clock::MockClock;

    use super::*;

    #[test]
    fn test_leaky_bucket_steady() {
        let mut rl = LeakyBucket::builder()
            .clock(MockClock::new_now())
            .burst(0)
            .rate(10)
            .build();

        for _ in 0..10 {
            for _ in 0..10 {
                assert!(rl.pass());
            }
            rl.forward(Duration::from_secs(1));
        }

        for _ in 0..10 {
            for _ in 0..10 {
                assert!(rl.pass());
            }
            assert!(!rl.pass());
            rl.forward(Duration::from_secs(1));
        }
    }

    #[test]
    fn test_leaky_bucket_burst() {
        let mut rl = LeakyBucket::builder()
            .clock(MockClock::new_now())
            .burst(10)
            .rate(10)
            .build();

        for _ in 0..10 {
            assert!(rl.pass());
        }

        for _ in 0..10 {
            for _ in 0..10 {
                assert!(rl.pass());
            }
            assert!(!rl.pass());
            rl.forward(Duration::from_secs(1));
        }
        for _ in 0..10 {
            for _ in 0..10 {
                assert!(rl.pass());
            }
            assert!(!rl.pass());
            rl.forward(Duration::from_secs(1));
        }

        rl.forward(Duration::from_secs(1000));

        for _ in 0..20 {
            assert!(rl.pass());
        }
        assert!(!rl.pass());
    }

    #[test]
    fn test_leaky_bucket_decorate() {
        let rl = LeakyBucket::builder()
            .clock(MockClock::new_now())
            .burst(10)
            .rate(10)
            .build();
        let mut v = 0;
        let f = |()| {
            v += 1;
            v
        };
        let mut f = rl.decorate(f);
        for i in 0..20 {
            assert_eq!(f(()).unwrap(), i + 1);
        }
        assert!(f(()).is_err());
        drop(f);
        assert_eq!(v, 20);
    }
}
