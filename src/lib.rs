#![feature(unboxed_closures, fn_traits)]
#![feature(integer_atomics)]

mod clock;
mod gcra;

pub use clock::Clock;
pub use gcra::{LeakyBucket, Policy, VirtualScheduling};
