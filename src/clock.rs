use governor::clock::{Clock, Reference};
use governor::nanos::Nanos;
use std::cell::RefCell;
use std::ops::Add;
use std::rc::Rc;

/// Clock source for using Redis as a limiter time base.
///
/// Uses `Rc<RefCell<redis::Connection>>` as `Clock` requires that `Clone` be implemented for the type.
pub struct RedisClock<C> {
    pub(crate) conn: Rc<RefCell<C>>,
    start_key: String,
}

// This impl is used because derive places a Clone bound on C,
// but Clone is satisfied for all Rc<T> regardless of whether
// T: Clone (and we don't want to force connections to be Clone)
impl<C> Clone for RedisClock<C> {
    fn clone(&self) -> Self {
        Self {
            conn: self.conn.clone(),
            start_key: self.start_key.clone(),
        }
    }
}

impl<C> RedisClock<C> {
    pub(crate) fn new(conn: Rc<RefCell<C>>, prefix: &str) -> Self {
        Self {
            conn,
            start_key: format!("{}:start", prefix),
        }
    }
}

impl<C> RedisClock<C>
where
    C: redis::ConnectionLike,
{
    fn now_nanos(conn: &mut C) -> u64 {
        let (secs, micros): (u64, u64) = redis::cmd("TIME")
            .query(conn)
            .expect("Failed to retrieve time from Redis");

        secs * 1_000_000_000 + micros * 1_000
    }

    pub(crate) fn reset_start(&self) {}
}

impl<C> Clock for RedisClock<C>
where
    C: redis::ConnectionLike,
{
    type Instant = RedisInstant;

    fn now(&self) -> Self::Instant {
        RedisInstant(Nanos::new(Self::now_nanos(&mut *self.conn.borrow_mut())))
    }

    fn start(&self) -> Self::Instant {
        let conn = &mut *self.conn.borrow_mut();
        redis_check_and_set!(conn, (&self.start_key) => {
            let start: Option<u64> = redis::Cmd::get(&self.start_key)
                .query(conn)
                .expect("Failed to check Redis for key presence");

            if let Some(start) = start {
                return RedisInstant(Nanos::new(start));
            }

            let now = Self::now_nanos(conn);

            let response: Option<(u64,)> = redis::pipe()
                .atomic()
                .cmd("SET")
                .arg(&self.start_key)
                .arg(now)
                .ignore()
                .cmd("GET")
                .arg(&self.start_key)
                .query(conn)
                .expect("Failed to set start time");

            match response {
                None => continue,
                Some((val,)) => return RedisInstant(Nanos::new(val)),
            }
        });
    }
}

/// An instance in time as observed by a connected Redis cluster.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct RedisInstant(Nanos);

impl Add<Nanos> for RedisInstant {
    type Output = RedisInstant;

    fn add(self, other: Nanos) -> RedisInstant {
        RedisInstant(self.0 + other)
    }
}

impl Reference for RedisInstant {
    fn duration_since(&self, earlier: Self) -> Nanos {
        self.0.saturating_sub(earlier.0)
    }

    fn saturating_sub(&self, duration: Nanos) -> Self {
        RedisInstant(self.0.saturating_sub(duration))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn obtains_reference_increments() {
        let redis = redis::Client::open("redis://127.0.0.1/").unwrap();
        let conn = redis.get_connection().unwrap();

        let conn = Rc::new(RefCell::new(conn));
        let clock = RedisClock::new(conn, &"test");

        let instant = clock.now();
        std::thread::sleep(Duration::from_millis(50));
        let instant2 = clock.now();

        assert!(instant2.0 > instant.0);
    }
}
