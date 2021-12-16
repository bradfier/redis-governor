#[macro_use]
extern crate log;

use std::borrow::Cow;
use std::cell::RefCell;
use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::marker::PhantomData;
use std::rc::Rc;

pub use governor;

use governor::middleware::NoOpMiddleware;
use governor::{Quota, RateLimiter};
use r2d2::{Pool, PooledConnection};
use redis::{Cmd, RedisResult, Value};

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

pub struct PooledRedisConnection(pub(crate) PooledConnection<redis::Client>);

impl redis::ConnectionLike for PooledRedisConnection {
    fn req_packed_command(&mut self, cmd: &[u8]) -> RedisResult<Value> {
        self.0.req_packed_command(cmd)
    }

    fn req_packed_commands(
        &mut self,
        cmd: &[u8],
        offset: usize,
        count: usize,
    ) -> RedisResult<Vec<Value>> {
        self.0.req_packed_commands(cmd, offset, count)
    }

    fn req_command(&mut self, cmd: &Cmd) -> RedisResult<Value> {
        self.0.req_command(cmd)
    }

    fn get_db(&self) -> i64 {
        self.0.get_db()
    }

    fn supports_pipelining(&self) -> bool {
        self.0.supports_pipelining()
    }

    fn check_connection(&mut self) -> bool {
        self.0.check_connection()
    }

    fn is_open(&self) -> bool {
        self.0.is_open()
    }
}

/// An instance of a Governor with a reserved connection to Redis.
pub struct GovernorInstance<C, K> {
    _conn: Rc<RefCell<C>>,
    state: state::RedisStateStore<C, K>,
    clock: clock::RedisClock<C>,
}

impl<C, K> GovernorInstance<C, K>
where
    K: Hash + Eq + Clone + Debug,
    C: redis::ConnectionLike,
{
    pub fn state(&self) -> &state::RedisStateStore<C, K> {
        &self.state
    }

    /// Get a reference to the stored [`RedisClock`](crate::clock::RedisClock).
    ///
    /// Useful to query the current time for displaying rate limiting information.
    pub fn clock(&self) -> &clock::RedisClock<C> {
        &self.clock
    }

    /// Wipe all of the rate limits for this governor.
    pub fn wipe(&self) {
        self.clock().reset_start();
        self.state().wipe();
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
        RateLimiter::new(quota, self.state().clone(), self.clock())
    }
}

/// A governor rate limiter using Redis as a distributed store.
///
/// The [`RedisGovernor`] acts as a factory for [`GovernorInstance`]s
/// which share an internal pool of Redis connections.
#[derive(Clone)]
pub struct RedisGovernor<K> {
    pool: Pool<redis::Client>,
    prefix: Cow<'static, str>,
    key: PhantomData<K>,
}

impl<K> Debug for RedisGovernor<K> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} [prefix={}]",
            std::any::type_name::<Self>(),
            self.prefix
        )
    }
}

impl<K> RedisGovernor<K>
where
    K: Hash + Eq + Clone + Debug,
{
    /// Create a new [`RedisGovernor`](Self) for an existing Redis client.
    ///
    /// The `prefix` argument allows multiple independent users of
    /// the governor (e.g. different services sharing a Redis instance)
    /// to prevent key collisions. The `prefix` will be cloned onto the heap if it
    /// is not a compile-time static string.
    pub fn new<I>(client: redis::Client, prefix: I) -> Self
    where
        I: Into<Cow<'static, str>>,
    {
        let prefix = prefix.into();

        Self {
            pool: r2d2::Pool::builder().build(client).unwrap(),
            prefix,
            key: Default::default(),
        }
    }

    pub fn instance(&self) -> GovernorInstance<PooledRedisConnection, K> {
        let conn = Rc::new(RefCell::new(PooledRedisConnection(
            self.pool.get().unwrap(),
        )));

        GovernorInstance {
            _conn: conn.clone(),
            state: state::RedisStateStore::new(conn.clone(), self.prefix.clone()),
            clock: clock::RedisClock::new(conn, &self.prefix),
        }
    }
}
