use test_log::test;

use governor::Quota;
use redis_governor::RedisGovernor;
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

#[test]
fn rate_limiter_works() {
    const LIMIT: u32 = 5u32;

    let redis = redis::Client::open("redis://127.0.0.1/").unwrap();
    let conn = redis.get_connection().unwrap();
    let quota = fixed_quota(LIMIT);

    let governor = RedisGovernor::new(conn, "basic-rate-limiter-test");
    governor.wipe();

    let redis_limiter = governor.rate_limiter(quota);

    for _ in 0..LIMIT {
        redis_limiter
            .check_key(&"test")
            .expect("unexpectedly rate limited");
    }

    redis_limiter
        .check_key(&"test")
        .expect_err("not rate limited when expected");
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
    let conn = redis.get_connection().unwrap();
    let governor = RedisGovernor::new(conn, PREFIX);
    governor.wipe();

    (0..THREADS)
        .map(|id| {
            let quota = quota;
            let redis = redis.clone();
            std::thread::Builder::new()
                .name(format!("concurrent-rate-limit-thread-{}", id))
                .spawn(move || {
                    // Each thread gets its own limiter conn for better testing
                    // and because the thread is not Send
                    let conn = redis.get_connection().unwrap();

                    let governor = RedisGovernor::new(conn, PREFIX);
                    let redis_limiter = governor.rate_limiter(quota);

                    for _ in 0..TRIES_PER_THREAD {
                        redis_limiter
                            .check_key(&"test")
                            .expect("unexpectedly rate limited");
                    }
                })
                .expect("failed to create thread")
        })
        .map(|h| h.join())
        .collect::<Result<Vec<()>, _>>()
        .expect("failure in testing thread");

    governor
        .rate_limiter(quota)
        .check_key(&"test")
        .expect_err("not rate limited when expected");
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
    let conn = redis.get_connection().unwrap();
    let governor = RedisGovernor::new(conn, PREFIX);
    governor.wipe();

    let mut results = vec![];

    for job in 0..JOBS {
        let key_name = format!("test-{}", job);

        results.extend((0..THREADS).map(|id| {
            let quota = quota;
            let redis = redis.clone();
            let key_name = key_name.clone();
            std::thread::Builder::new()
                .name(format!("disjoint-rate-limit-job-{}-thread-{}", job, id))
                .spawn(move || {
                    let key_name_ref = key_name.as_str();
                    let conn = redis.get_connection().unwrap();

                    let governor = RedisGovernor::new(conn, PREFIX);
                    let redis_limiter = governor.rate_limiter(quota);

                    for _ in 0..TRIES_PER_THREAD {
                        redis_limiter
                            .check_key(&key_name_ref)
                            .expect("unexpectedly rate limited");
                    }
                })
                .expect("failed to create thread")
        }));
    }

    results
        .into_iter()
        .map(|h| h.join())
        .collect::<Result<(), _>>()
        .expect("failure in testing thread");

    for job in 0..JOBS {
        let key_name = format!("test-{}", job);

        governor
            .rate_limiter(quota)
            .check_key(&key_name.as_str())
            .expect_err("not rate limited when expected");
    }
}
