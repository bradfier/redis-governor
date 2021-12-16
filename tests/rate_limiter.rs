use test_log::test;

use governor::{Quota, RateLimiter};
use redis_governor::{
    clock::RedisClock, state::RedisStateStore, PooledRedisConnection, RedisGovernor,
    RedisNoOpMiddleware,
};
use std::fmt::Debug;
use std::hash::Hash;
use std::num::NonZeroU32;
use std::time::Duration;

/// Fixed quota to ensure that test duration variance/timing cannot
/// cause flakiness.
fn fixed_quota(limit: u32) -> Quota {
    const DAY_IN_SECS: u64 = 60 * 60;

    // Use 1 hour as an arbitrarily long period to prevent bucket leaking.
    Quota::with_period(Duration::from_secs(DAY_IN_SECS))
        .expect("failed to create quota")
        .allow_burst(NonZeroU32::new(limit).expect("limit must be greater than zero"))
}

type RedisRateLimiter<K> = RateLimiter<
    K,
    RedisStateStore<PooledRedisConnection, K>,
    RedisClock<PooledRedisConnection>,
    RedisNoOpMiddleware,
>;

fn should_rate_limit<K: Hash + Debug + Eq + Clone>(limiter: &RedisRateLimiter<K>, key: &K) {
    let _ = limiter
        .check_key(key)
        .expect_err("not rate limited when expected");
}

fn should_not_rate_limit<K: Hash + Debug + Eq + Clone>(limiter: &RedisRateLimiter<K>, key: &K) {
    let _ = limiter.check_key(key).expect("unexpectedly rate limited");
}

#[test]
fn rate_limiter_works() {
    const LIMIT: u32 = 5u32;

    let redis = redis::Client::open("redis://127.0.0.1/").unwrap();
    let quota = fixed_quota(LIMIT);

    let pooled_governor = RedisGovernor::new(redis, "basic-rate-limiter-test");
    let governor = pooled_governor.instance();
    governor.wipe();

    let redis_limiter = governor.rate_limiter(quota);

    for _ in 0..LIMIT {
        should_not_rate_limit(&redis_limiter, &"test");
    }

    should_rate_limit(&redis_limiter, &"test");
}

#[test]
fn rate_limiter_can_recover() {
    const MINUTELY_LIMIT: u32 = 12u32;

    let redis = redis::Client::open("redis://127.0.0.1/").unwrap();
    let quota = Quota::per_minute(NonZeroU32::new(MINUTELY_LIMIT).unwrap());

    let pooled_governor = RedisGovernor::new(redis, "rate-limiter-recovery-test");
    let governor = pooled_governor.instance();
    governor.wipe();

    let redis_limiter = governor.rate_limiter(quota);

    for _ in 0..MINUTELY_LIMIT {
        should_not_rate_limit(&redis_limiter, &"test");
    }

    // Ensure over the limit
    for _ in 0..MINUTELY_LIMIT {
        let _ = redis_limiter.check_key(&"test");
    }

    should_rate_limit(&redis_limiter, &"test");

    std::thread::sleep(Duration::from_secs_f32((60 / MINUTELY_LIMIT) as f32));

    should_not_rate_limit(&redis_limiter, &"test");
}

#[test]
fn rate_limiter_works_when_contended() {
    const THREADS: u32 = 10;
    const LIMIT: u32 = 10000;
    const TRIES_PER_THREAD: u32 = LIMIT / THREADS;
    const PREFIX: &str = "concurrent-rate-limiter-test";

    // Use fixed quota so spillover is not possible which would cause test flakes
    let quota = fixed_quota(LIMIT);
    let redis = redis::Client::open("redis://127.0.0.1/").unwrap();
    let pool = RedisGovernor::new(redis, PREFIX);
    let governor = pool.instance();
    governor.wipe();

    (0..THREADS)
        .map(|id| {
            let quota = quota;
            let pool = pool.clone();
            std::thread::Builder::new()
                .name(format!("concurrent-rate-limit-thread-{}", id))
                .spawn(move || {
                    // Each thread gets its own limiter conn for better testing
                    // and because the thread is not Send
                    let governor = pool.instance();
                    let redis_limiter = governor.rate_limiter(quota);

                    for _ in 0..TRIES_PER_THREAD {
                        should_not_rate_limit(&redis_limiter, &"test");
                    }
                })
                .expect("failed to create thread")
        })
        .map(|h| h.join())
        .collect::<Result<Vec<()>, _>>()
        .expect("failure in testing thread");

    should_rate_limit(&governor.rate_limiter(quota), &"test");
}

#[test]
fn can_maintain_disjoint_rate_limits() {
    const JOBS: u32 = 2;
    const THREADS: u32 = 5;
    const LIMIT: u32 = 200;
    const TRIES_PER_THREAD: u32 = LIMIT / THREADS;
    const PREFIX: &str = "disjoint-rate-limiter-test";

    // Use fixed quota so spillover is not possible which would cause test flakes
    let quota = fixed_quota(LIMIT);
    let redis = redis::Client::open("redis://127.0.0.1/").unwrap();
    let pool = RedisGovernor::new(redis, PREFIX);
    let governor = pool.instance();
    governor.wipe();

    let mut results = vec![];

    for job in 0..JOBS {
        let key_name = format!("test-{}", job);

        results.extend((0..THREADS).map(|id| {
            let quota = quota;
            let pool = pool.clone();
            let key_name = key_name.clone();
            std::thread::Builder::new()
                .name(format!("disjoint-rate-limit-job-{}-thread-{}", job, id))
                .spawn(move || {
                    let governor = pool.instance();
                    let redis_limiter = governor.rate_limiter(quota);

                    for _ in 0..TRIES_PER_THREAD {
                        should_not_rate_limit(&redis_limiter, &key_name.as_str());
                    }
                })
                .expect("failed to create thread")
        }));
    }

    results
        .into_iter()
        .try_for_each(|h| h.join())
        .expect("failure in testing thread");

    for job in 0..JOBS {
        let key_name = format!("test-{}", job);

        should_rate_limit(&governor.rate_limiter(quota), &(key_name.as_str()));
    }
}
