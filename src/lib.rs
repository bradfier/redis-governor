#[macro_use]
extern crate log;

use std::borrow::Cow;
use std::cell::RefCell;
use std::fmt::Debug;
use std::hash::Hash;
use std::rc::Rc;

use governor::middleware::NoOpMiddleware;
use governor::{Quota, RateLimiter};

pub mod clock;
pub mod state;

/// A [`NoOpMiddleware`](governor::middleware::NoOpMiddleware) usable when using
/// [`RedisClock`](crate::clock::RedisClock) as a clock source.
pub type RedisNoOpMiddleware = NoOpMiddleware<clock::RedisInstant>;

/// A governor rate limiter using Redis as a distributed store.
///
/// The [`RedisGovernor`] acts as a factory for [`RateLimiter`](governor::RateLimiter)s
/// which share a single underlying Redis connection.
pub struct RedisGovernor<K>
where
    K: Hash + Eq + Clone + Debug,
{
    clock: clock::RedisClock,
    state: state::RedisStateStore<K>,
}

impl<K> RedisGovernor<K>
where
    K: Hash + Eq + Clone + Debug,
{
    /// Create a new [`RedisGovernor`](Self) for an existing Redis connection.
    ///
    /// The `prefix` argument allows multiple independent users of
    /// the governor (e.g. different services sharing a Redis instance)
    /// to prevent key collisions. The `prefix` will be cloned onto the heap if it
    /// is not a compile-time static string.
    pub fn new<I>(conn: redis::Connection, prefix: I) -> Self
    where
        I: Into<Cow<'static, str>>,
    {
        let conn = Rc::new(RefCell::new(conn));
        let state = state::RedisStateStore::new(conn.clone(), prefix);
        let clock = clock::RedisClock(conn);

        Self { clock, state }
    }

    /// Wipe all of the rate limits for this governor.
    pub fn wipe(&self) {
        self.state.wipe();
    }

    /// Create a new [`RateLimiter`](governor::RateLimiter) with a given [`Quota`](governor::Quota).
    ///
    /// The idea is that you may want to create many different rate limits, but be able
    /// to reuse the same Redis connection for all of them.
    pub fn rate_limiter(
        &self,
        quota: Quota,
    ) -> RateLimiter<K, state::RedisStateStore<K>, clock::RedisClock, RedisNoOpMiddleware> {
        RateLimiter::new(quota, self.state.clone(), &self.clock)
    }
}
