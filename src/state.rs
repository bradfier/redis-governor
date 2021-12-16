use governor::nanos::Nanos;
use governor::state::StateStore;
use siphasher::sip::SipHasher;
use std::borrow::Cow;
use std::cell::RefCell;

use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};

use std::rc::Rc;

/// Governor state store backed by a Redis instance
///
/// The state store uses a single Redis hash as the canonical
/// store for state values, with separate individual values used
/// as semaphores to manage lock contention, as Redis cannot directly
/// use hash entries with it's provided conflict-detection mechanism.
pub struct RedisStateStore<C, K> {
    conn: Rc<RefCell<C>>,
    pub(crate) prefix: Cow<'static, str>,
    hash_key: String,
    key: PhantomData<K>,
}

// Provide a Clone impl that does not require Clone by
// simply duping the Rc (which the derive will not do because it
// puts a Clone bound on all params)
impl<C, K> Clone for RedisStateStore<C, K> {
    fn clone(&self) -> Self {
        Self {
            conn: self.conn.clone(),
            prefix: self.prefix.clone(),
            hash_key: self.hash_key.clone(),
            key: Default::default(),
        }
    }
}

impl<C, K> RedisStateStore<C, K>
where
    C: redis::ConnectionLike,
{
    pub(crate) fn new<I: Into<Cow<'static, str>>>(conn: Rc<RefCell<C>>, prefix: I) -> Self {
        let prefix = prefix.into();
        Self {
            conn,
            hash_key: format!("{}:governor:hash", prefix),
            prefix,
            key: Default::default(),
        }
    }

    pub(crate) fn wipe(&self) {
        redis::Cmd::del(&self.hash_key).execute(self.conn.deref().borrow_mut().deref_mut());
    }
}

impl<C, K: Hash> RedisStateStore<C, K> {
    fn key_hash(&self, key: &K) -> String {
        let mut hasher = SipHasher::new();
        key.hash(&mut hasher);
        format!("{:x}", hasher.finish())
    }
}

impl<C, K> StateStore for RedisStateStore<C, K>
where
    C: redis::ConnectionLike,
    K: Hash + Eq + Clone + Debug,
    // {
    //     type Key = K;
    //
    //     fn measure_and_replace<T, F, E>(&self, key: &Self::Key, f: F) -> Result<T, E>
    //     where
    //         F: Fn(Option<Nanos>) -> Result<(T, Nanos), E>,
    //     {
    //         trace!("Measure and replace for {:?}", key);
    //         let hash = self.key_hash(key);
    //         // We need a separate individual value key, as Redis can't WATCH a HASH field,
    //         // but equally finding all the values with a given prefix is O(n),
    //         // whereas it's O(1) for a Hash.
    //         let value_key = format!("{}:governor:value:{}", self.prefix, &hash);
    //
    //         let conn = &mut *self.conn.borrow_mut();
    //
    //         // This loop will effectively attempt to set the Redis key
    //         // by doing check-and-set attempts until it "wins", similar to
    //         // reference implementations used in governor.
    //         loop {
    //             trace!("Check and set attempt");
    //             // WATCH value field
    //             // This will abort the atomic section later if the semaphore key is updated
    //             // by another connection.
    //             // WATCHes are always cancelled after an EXEC command, so it needs
    //             // to be performed every iteration.
    //             let _: () = redis::cmd("WATCH")
    //                 .arg(&value_key)
    //                 .query(conn)
    //                 .expect("Failed to watch for key");
    //
    //             // Obtain previous value from state store.
    //             let prev: Option<u64> = redis::Cmd::hget(&self.hash_key, &hash)
    //                 .query(conn)
    //                 .expect("Failed to check Redis for key presence");
    //             trace!("Previous value: {:?}", prev);
    //             let decision = f(prev.map(Into::into));
    //
    //             match decision {
    //                 Ok((result, new_data)) => {
    //                     trace!("Updated, setting data to {:?}", new_data);
    //                     // The atomic block sets the value key to trigger the semaphore
    //                     // and then HSETs the store key in the hashtable which is used
    //                     // as the actual store
    //                     let new_data: u64 = new_data.into();
    //                     let response: Option<()> = redis::pipe()
    //                         .atomic()
    //                         .set(&value_key, new_data)
    //                         .ignore()
    //                         .hset(&self.hash_key, &hash, new_data)
    //                         .query(conn)
    //                         .expect("Failed to run atomic section");
    //
    //                     match response {
    //                         // The request was successful
    //                         Some(()) => return Ok(result),
    //                         None => {
    //                             trace!("Key update conflict for {:?}, retrying", key);
    //                             continue;
    //                         }
    //                     }
    //                 }
    //                 Err(_) => {
    //                     trace!("Error setting key");
    //                     return decision.map(|(result, _)| result);
    //                 }
    //             }
    //         }
    //     }
    // }
{
    type Key = K;

    fn measure_and_replace<T, F, E>(&self, key: &Self::Key, f: F) -> Result<T, E>
    where
        F: Fn(Option<Nanos>) -> Result<(T, Nanos), E>,
    {
        trace!("Measure and replace for {:?}", key);
        let hash = self.key_hash(key);
        // We need a separate individual value key, as Redis can't WATCH a HASH field,
        // but equally finding all the values with a given prefix is O(n),
        // whereas it's O(1) for a Hash.
        let value_key = format!("{}:governor:value:{}", self.prefix, &hash);

        // This loop will effectively attempt to set the Redis key
        // by doing check-and-set attempts until it "wins", similar to
        // reference implementations used in governor.
        let mut conn = self.conn.deref().borrow_mut();
        redis_check_and_set!(conn.deref_mut(), (&value_key) => {
            // Obtain previous value from state store.
            let prev: Option<u64> = redis::Cmd::hget(&self.hash_key, &hash)
                .query(conn.deref_mut())
                .expect("Failed to check Redis for key presence");
            trace!("Previous value: {:?}", prev);
            let decision = f(prev.map(Into::into));

            match decision {
                Ok((result, new_data)) => {
                    trace!("Updated, setting data to {:?}", new_data);
                    // The atomic block sets the value key to trigger the semaphore
                    // and then HSETs the store key in the hashtable which is used
                    // as the actual store
                    let new_data: u64 = new_data.into();
                    let response: Option<()> = redis::pipe()
                        .atomic()
                        .set(&value_key, new_data)
                        .ignore()
                        .hset(&self.hash_key, &hash, new_data)
                        .query(conn.deref_mut())
                        .expect("Failed to run atomic section");

                    match response {
                        // The request was successful
                        Some(()) => return Ok(result),
                        None => {
                            trace!("Key update conflict for {:?}, retrying", key);
                            continue;
                        }
                    }
                }
                Err(_) => {
                    trace!("Error setting key");
                    return decision.map(|(result, _)| result);
                }
            }
        })
    }
}
