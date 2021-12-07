use governor::nanos::Nanos;
use governor::state::StateStore;
use redis::Connection;
use std::cell::RefCell;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use std::rc::Rc;
use siphasher::sip::SipHasher;
use std::borrow::BorrowMut;
use std::marker::PhantomData;

struct RedisStateStore<K> {
    conn: Rc<RefCell<Connection>>,
    prefix: &'static str,
    key: PhantomData<K>,
}

impl<K> RedisStateStore<K> {
    fn key_hash(&self, key: &K) -> String {
        let mut hasher = SipHasher::new();
        key.hash(&mut hasher);
        format!("{:x}", hasher.finish())
    }
}

impl<K: Hash + Eq + Clone> StateStore for RedisStateStore<K> {
    type Key = K;

    fn measure_and_replace<T, F, E>(&self, key: &Self::Key, f: F) -> Result<T, E>
    where
        F: Fn(Option<Nanos>) -> Result<(T, Nanos), E>,
    {
        let hash = self.key_hash(key);
        // We need both of these as Redis can't WATCH a HASH field, but equally finding
        // all the values with a given prefix is O(n) whereas it's O(1) for a Hash.
        let hash_key = format!("{}:governor:hash", self.prefix);
        let value_key = format!("{}:governor:value:{}", self.prefix, &hash);

        let exists = redis::Cmd::exists(&value_key).query(&mut *self.conn.borrow_mut())
            .expect("Failed to check Redis for key presence");

        // Begin Redis transaction
        // WATCH value field

        // Get previous, pass to F

        // Set value and EXEC - logging but ignoring error due to conflict
    }
}
