use governor::clock::{Clock, Reference};
use governor::nanos::Nanos;
use std::cell::RefCell;
use std::ops::Add;
use std::rc::Rc;

/// Clock source for using Redis as a limiter time base.
///
/// Uses `Rc<RefCell<redis::Connection>>` as `Clock` requires that `Clone` be implemented for the type.
pub struct RedisClock<C>(pub(crate) Rc<RefCell<C>>);

impl<C> Clone for RedisClock<C> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<C> Clock for RedisClock<C>
where
    C: redis::ConnectionLike,
{
    type Instant = RedisInstant;

    fn now(&self) -> Self::Instant {
        let (secs, micros): (u64, u64) = redis::cmd("TIME")
            .query(&mut *self.0.borrow_mut())
            .expect("Failed to retrieve time from Redis");

        RedisInstant(Nanos::new(secs * 1_000_000_000 + micros * 1_000))
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

        let clock = Rc::new(RefCell::new(conn));
        let clock = RedisClock(clock);

        let instant = clock.now();
        std::thread::sleep(Duration::from_millis(50));
        let instant2 = clock.now();

        assert!(instant2.0 > instant.0);
    }
}
