//! Implementation of generic cell rate algorithm(https://en.wikipedia.org/wiki/Generic_cell_rate_algorithm)

use std::time::Duration;

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
    pub fn decorate<'a, Req, Resp>(
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

pub struct VirtualScheduling<C = SystemClock> {
    clock: C,
    tat: Mutex<u64>, // theorical arrival time
    tolerance: u64,
    gap: u64,
}

impl VirtualScheduling {
    pub fn builder() -> VirtualSchedulingBuilder<SystemClock> {
        VirtualSchedulingBuilder {
            clock: SystemClock,
            tolerance: 0,
            gap: 0,
        }
    }
}

impl<C> Policy for VirtualScheduling<C>
where
    C: Clock,
{
    fn pass(&self) -> bool {
        let now = self.clock.now();
        let mut tat = self.tat.lock();
        if now + self.tolerance < *tat {
            false
        } else {
            *tat = std::cmp::max(*tat, now) + self.gap;
            true
        }
    }
}

impl<C> VirtualScheduling<C>
where
    C: Clock,
{
    pub fn decorate<'a, Req, Resp>(
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

impl VirtualScheduling<MockClock> {
    pub fn forward(&mut self, dur: Duration) {
        self.clock.forward(dur);
    }

    pub fn backward(&mut self, dur: Duration) {
        self.clock.backward(dur);
    }
}

pub struct VirtualSchedulingBuilder<C> {
    clock: C,
    tolerance: u64,
    gap: u64,
}

impl<C> VirtualSchedulingBuilder<C> {
    pub fn clock<NC>(self, clock: NC) -> VirtualSchedulingBuilder<NC> {
        VirtualSchedulingBuilder {
            clock,
            tolerance: self.tolerance,
            gap: self.gap,
        }
    }

    pub fn tolerance(mut self, tolerance: Duration) -> Self {
        self.tolerance = tolerance.as_millis() as u64;
        self
    }

    pub fn gap(mut self, gap: Duration) -> Self {
        self.gap = gap.as_millis() as u64;
        self
    }

    pub fn build(self) -> VirtualScheduling<C> {
        VirtualScheduling {
            clock: self.clock,
            tat: Mutex::new(0),
            tolerance: self.tolerance,
            gap: self.gap,
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

    #[test]
    fn test_virtual_schuduling_steady() {
        let mut rl = VirtualScheduling::builder()
            .clock(MockClock::new_now())
            .tolerance(Duration::from_secs(0))
            .gap(Duration::from_millis(100))
            .build();

        for _ in 0..10 {
            for _ in 0..10 {
                rl.forward(Duration::from_millis(100));
                assert!(rl.pass());
            }
            assert!(!rl.pass());
            rl.forward(Duration::from_secs(1));
        }
    }

    #[test]
    fn test_virtual_schuduling_tolerance() {
        let mut rl = VirtualScheduling::builder()
            .clock(MockClock::new_now())
            .tolerance(Duration::from_millis(500))
            .gap(Duration::from_secs(1))
            .build();

        assert!(rl.pass());
        rl.forward(Duration::from_millis(500));
        assert!(rl.pass());
        rl.forward(Duration::from_millis(500));
        assert!(!rl.pass());
        rl.forward(Duration::from_millis(500));
        assert!(rl.pass());
        rl.forward(Duration::from_secs(1));
        assert!(rl.pass());
    }
}
