//! Bounded in-memory query-embedding LRU with TTL (spec 12.3). Never persisted; key includes the profile id.

use std::collections::HashMap;

pub struct QueryCache {
    cap: usize,
    ttl_ms: i64,
    tick: u64,
    map: HashMap<(String, String), Entry>,
}

struct Entry {
    vec: Vec<f32>,
    expires_ms: i64,
    used: u64,
}

impl QueryCache {
    pub fn new(cap: usize, ttl_ms: i64) -> Self {
        Self {
            cap,
            ttl_ms,
            tick: 0,
            map: HashMap::new(),
        }
    }

    pub fn get(&mut self, profile: &str, text: &str, now_ms: i64) -> Option<Vec<f32>> {
        self.tick += 1;
        let key = (profile.to_string(), text.to_string());
        match self.map.get_mut(&key) {
            Some(e) if e.expires_ms > now_ms => {
                e.used = self.tick;
                Some(e.vec.clone())
            }
            Some(_) => {
                self.map.remove(&key);
                None
            }
            None => None,
        }
    }

    pub fn put(&mut self, profile: &str, text: &str, vec: Vec<f32>, now_ms: i64) {
        if self.cap == 0 {
            return;
        }
        self.tick += 1;
        self.map.retain(|_, e| e.expires_ms > now_ms);
        let key = (profile.to_string(), text.to_string());
        if !self.map.contains_key(&key) && self.map.len() >= self.cap {
            // ponytail: O(cap) eviction scan; cap is 256, switch to a linked list only if it grows.
            if let Some(k) = self
                .map
                .iter()
                .min_by_key(|(_, e)| e.used)
                .map(|(k, _)| k.clone())
            {
                self.map.remove(&k);
            }
        }
        self.map.insert(
            key,
            Entry {
                vec,
                expires_ms: now_ms + self.ttl_ms,
                used: self.tick,
            },
        );
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lru_ttl_and_profile_key() {
        let mut c = QueryCache::new(2, 1000);
        c.put("p", "a", vec![1.0], 0);
        c.put("p", "b", vec![2.0], 0);
        assert!(c.get("p", "a", 1).is_some()); // a is now most recent
        c.put("p", "c", vec![3.0], 2); // evicts b
        assert!(c.get("p", "b", 3).is_none());
        assert!(c.get("p", "a", 3).is_some());
        assert!(c.get("other", "a", 3).is_none());
        assert!(c.get("p", "a", 1001).is_none()); // expired
        assert!(c.len() <= 2);
    }
}
