#[macro_use]
extern crate log;

use std::borrow::Cow;
use std::cell::RefCell;
use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::rc::Rc;

pub use governor;

use governor::middleware::NoOpMiddleware;
use governor::{Quota, RateLimiter};

#[macro_use]
mod private_macros {
    macro_rules! redis_check_and_set {
        ($conn:expr, ($watch_key:expr) => $block:expr) => {
            loop {
                // WATCH value field
                // This will abort the atomic section later if the semaphore key is updated
                // by another connection.
                // WATCHes are always cancelled after an EXEC command, so it needs
                // to be performed every iteration.
                let _: () = redis::cmd("WATCH")
                    .arg($watch_key)
                    .query($conn)
                    .expect("Failed to watch for key");

                {
                    $block
                }
            }
        };
    }
}

pub mod clock;
pub mod state;

/// A [`NoOpMiddleware`](governor::middleware::NoOpMiddleware) usable when using
/// [`RedisClock`](crate::clock::RedisClock) as a clock source.
pub type RedisNoOpMiddleware = NoOpMiddleware<clock::RedisInstant>;

/// A governor rate limiter using Redis as a distributed store.
///
/// The [`RedisGovernor`] acts as a factory for [`RateLimiter`](governor::RateLimiter)s
/// which share a single underlying Redis connection.
pub struct RedisGovernor<C, K> {
    _clock: clock::RedisClock<C>,
    state: state::RedisStateStore<C, K>,
}

impl<C, K> Clone for RedisGovernor<C, K> {
    fn clone(&self) -> Self {
        Self {
            _clock: self._clock.clone(),
            state: self.state.clone(),
        }
    }
}

impl<C, K> Debug for RedisGovernor<C, K> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} [prefix={}]",
            std::any::type_name::<Self>(),
            self.state.prefix
        )
    }
}

impl<C, K> RedisGovernor<C, K>
where
    C: redis::ConnectionLike,
    K: Hash + Eq + Clone + Debug,
{
    /// Create a new [`RedisGovernor`](Self) for an existing Redis connection.
    ///
    /// The `prefix` argument allows multiple independent users of
    /// the governor (e.g. different services sharing a Redis instance)
    /// to prevent key collisions. The `prefix` will be cloned onto the heap if it
    /// is not a compile-time static string.
    pub fn new<I>(conn: C, prefix: I) -> Self
    where
        I: Into<Cow<'static, str>>,
    {
        let prefix = prefix.into();
        let conn = Rc::new(RefCell::new(conn));
        let clock = clock::RedisClock::new(conn.clone(), prefix.as_ref());
        let state = state::RedisStateStore::new(conn, prefix);

        Self {
            _clock: clock,
            state,
        }
    }

    /// Wipe all of the rate limits for this governor.
    pub fn wipe(&self) {
        self._clock.reset_start();
        self.state.wipe();
    }

    /// Get a reference to the stored [`RedisClock`](crate::clock::RedisClock).
    ///
    /// Useful to query the current time for displaying rate limiting information.
    pub fn clock(&self) -> &clock::RedisClock<C> {
        &self._clock
    }

    /// Create a new [`RateLimiter`](governor::RateLimiter) with a given [`Quota`](governor::Quota).
    ///
    /// The idea is that you may want to create many different rate limits, but be able
    /// to reuse the same Redis connection for all of them.
    pub fn rate_limiter(
        &self,
        quota: Quota,
    ) -> RateLimiter<K, state::RedisStateStore<C, K>, clock::RedisClock<C>, RedisNoOpMiddleware>
    {
        RateLimiter::new(quota, self.state.clone(), &self._clock)
    }
}
