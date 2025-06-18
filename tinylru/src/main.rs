use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex, MutexGuard};

// Default size for the LRU cache
const DEFAULT_SIZE: usize = 256;

// Internal LRU item structure
struct LruItem<K, V> {
    key: K,
    value: V,
    prev: Option<usize>,
    next: Option<usize>,
}

// Main LRU cache structure
pub struct LRU<K, V> {
    size: usize,
    items: HashMap<K, usize>,
    entries: Vec<LruItem<K, V>>,
    head: Option<usize>,
    tail: Option<usize>,
    free_list: Vec<usize>,
}

// Thread-safe wrapper for the LRU
#[derive(Clone)]
pub struct ConcurrentLRU<K, V> {
    inner: Arc<Mutex<LRU<K, V>>>,
}

impl<K: Eq + Hash + Clone, V: Clone> LRU<K, V> {
    // Create a new LRU with default size
    pub fn new() -> Self {
        Self::with_size(DEFAULT_SIZE)
    }

    // Create a new LRU with specified size
    pub fn with_size(size: usize) -> Self {
        if size == 0 {
            panic!("invalid size");
        }
        Self {
            size,
            items: HashMap::new(),
            entries: Vec::new(),
            head: None,
            tail: None,
            free_list: Vec::new(),
        }
    }

    // Resize the LRU, evicting items if necessary
    pub fn resize(&mut self, size: usize) -> (Vec<K>, Vec<V>) {
        if size == 0 {
            panic!("invalid size");
        }

        let mut evicted_keys = Vec::new();
        let mut evicted_values = Vec::new();

        while size < self.items.len() {
            if let Some((key, value)) = self.evict() {
                evicted_keys.push(key);
                evicted_values.push(value);
            }
        }

        self.size = size;
        (evicted_keys, evicted_values)
    }

    // Get current length
    pub fn len(&self) -> usize {
        self.items.len()
    }

    // Check if empty
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    // Set or replace a value with eviction info
    pub fn set_evicted(
        &mut self,
        key: K,
        value: V,
    ) -> (Option<V>, bool, Option<K>, Option<V>, bool) {
        if let Some(index) = self.items.get(&key) {
            // Key already exists - replace value
            let index = *index;
            let prev_value = self.entries[index].value.clone();
            self.entries[index].value = value;
            self.move_to_front(index);
            (Some(prev_value), true, None, None, false)
        } else {
            // Key doesn't exist - insert new entry
            let evicted = if self.items.len() >= self.size {
                self.evict()
            } else {
                None
            };

            let index = self.allocate_entry(key.clone(), value);
            self.items.insert(key, index);
            self.push_front(index);

            match evicted {
                Some((k, v)) => (None, false, Some(k), Some(v), true),
                None => (None, false, None, None, false),
            }
        }
    }

    // Set or replace a value
    pub fn set(&mut self, key: K, value: V) -> (Option<V>, bool) {
        let (prev, replaced, _, _, _) = self.set_evicted(key, value);
        (prev, replaced)
    }

    // Get a value and mark as recently used
    pub fn get(&mut self, key: &K) -> Option<V> {
        let index = match self.items.get(key) {
            Some(&index) => index,
            None => return None,
        };

        let value = self.entries[index].value.clone();
        self.move_to_front(index);
        Some(value)
    }

    // Check if key exists
    pub fn contains(&self, key: &K) -> bool {
        self.items.contains_key(key)
    }

    // Peek at a value without marking as recently used
    pub fn peek(&self, key: &K) -> Option<V> {
        self.items
            .get(key)
            .map(|&index| self.entries[index].value.clone())
    }

    // Delete a key-value pair
    pub fn delete(&mut self, key: &K) -> (Option<V>, bool) {
        if let Some(index) = self.items.remove(key) {
            let value = self.entries[index].value.clone();
            self.remove_entry(index);
            (Some(value), true)
        } else {
            (None, false)
        }
    }

    // Clear all entries
    pub fn clear(&mut self) {
        self.items.clear();
        self.entries.clear();
        self.head = None;
        self.tail = None;
        self.free_list.clear();
    }

    // Iterate from most to least recently used
    pub fn range<F>(&self, mut iter: F)
    where
        F: FnMut(&K, &V) -> bool,
    {
        let mut current = self.head;
        while let Some(index) = current {
            let entry = &self.entries[index];
            if !iter(&entry.key, &entry.value) {
                return;
            }
            current = entry.next;
        }
    }

    // Iterate from least to most recently used
    pub fn reverse<F>(&self, mut iter: F)
    where
        F: FnMut(&K, &V) -> bool,
    {
        let mut current = self.tail;
        while let Some(index) = current {
            let entry = &self.entries[index];
            if !iter(&entry.key, &entry.value) {
                return;
            }
            current = entry.prev;
        }
    }

    // Internal: Evict least recently used item
    fn evict(&mut self) -> Option<(K, V)> {
        self.tail.map(|tail| {
            let entry = &self.entries[tail];
            let key = entry.key.clone();
            let value = entry.value.clone();
            self.items.remove(&key);
            self.remove_entry(tail);
            (key, value)
        })
    }

    // Internal: Move an entry to the front
    fn move_to_front(&mut self, index: usize) {
        if self.head == Some(index) {
            return;
        }
        self.remove_entry(index);
        self.push_front(index);
    }

    // Internal: Remove an entry from the linked list (but keep in entries vec)
    fn remove_entry(&mut self, index: usize) {
        let prev = self.entries[index].prev;
        let next = self.entries[index].next;

        if let Some(prev) = prev {
            self.entries[prev].next = next;
        } else {
            self.head = next;
        }

        if let Some(next) = next {
            self.entries[next].prev = prev;
        } else {
            self.tail = prev;
        }

        self.free_list.push(index);
    }

    // Internal: Push an entry to the front
    fn push_front(&mut self, index: usize) {
        self.entries[index].prev = None;
        self.entries[index].next = self.head;

        if let Some(head) = self.head {
            self.entries[head].prev = Some(index);
        } else {
            self.tail = Some(index);
        }

        self.head = Some(index);
    }

    // Internal: Allocate a new entry
    fn allocate_entry(&mut self, key: K, value: V) -> usize {
        if let Some(index) = self.free_list.pop() {
            self.entries[index] = LruItem {
                key,
                value,
                prev: None,
                next: None,
            };
            index
        } else {
            let index = self.entries.len();
            self.entries.push(LruItem {
                key,
                value,
                prev: None,
                next: None,
            });
            index
        }
    }
}

impl<K: Eq + Hash + Clone, V: Clone> Default for LRU<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Eq + Hash + Clone + Send + 'static, V: Clone + Send + 'static> ConcurrentLRU<K, V> {
    pub fn new() -> Self {
        Self::with_size(DEFAULT_SIZE)
    }

    pub fn with_size(size: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(LRU::with_size(size))),
        }
    }

    pub fn resize(&self, size: usize) -> (Vec<K>, Vec<V>) {
        self.lock().resize(size)
    }

    pub fn len(&self) -> usize {
        self.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    pub fn set_evicted(&self, key: K, value: V) -> (Option<V>, bool, Option<K>, Option<V>, bool) {
        self.lock().set_evicted(key, value)
    }

    pub fn set(&self, key: K, value: V) -> (Option<V>, bool) {
        self.lock().set(key, value)
    }

    pub fn get(&self, key: &K) -> Option<V> {
        self.lock().get(key)
    }

    pub fn contains(&self, key: &K) -> bool {
        self.lock().contains(key)
    }

    pub fn peek(&self, key: &K) -> Option<V> {
        self.lock().peek(key)
    }

    pub fn delete(&self, key: &K) -> (Option<V>, bool) {
        self.lock().delete(key)
    }

    pub fn clear(&self) {
        self.lock().clear()
    }

    pub fn range<F>(&self, iter: F)
    where
        F: FnMut(&K, &V) -> bool + Send + 'static,
    {
        self.lock().range(iter)
    }

    pub fn reverse<F>(&self, iter: F)
    where
        F: FnMut(&K, &V) -> bool + Send + 'static,
    {
        self.lock().reverse(iter)
    }

    fn lock(&self) -> MutexGuard<LRU<K, V>> {
        self.inner.lock().unwrap()
    }
}

impl<K: Eq + Hash + Clone + Send + 'static, V: Clone + Send + 'static> Default
    for ConcurrentLRU<K, V>
{
    fn default() -> Self {
        Self::new()
    }
}

// Main function demonstrating usage
fn main() {
    // Create a new LRU cache with size 3
    let lru = ConcurrentLRU::<i32, String>::with_size(3);

    // Set some values
    let (prev, replaced) = lru.set(1, "one".to_string());
    println!("Set 1: prev={:?}, replaced={}", prev, replaced);

    let (prev, replaced) = lru.set(2, "two".to_string());
    println!("Set 2: prev={:?}, replaced={}", prev, replaced);

    let (prev, replaced) = lru.set(3, "three".to_string());
    println!("Set 3: prev={:?}, replaced={}", prev, replaced);

    // This will evict the least recently used item (1)
    let (prev, replaced, evicted_key, evicted_value, evicted) =
        lru.set_evicted(4, "four".to_string());
    println!(
        "Set 4: prev={:?}, replaced={}, evicted_key={:?}, evicted_value={:?}, evicted={}",
        prev, replaced, evicted_key, evicted_value, evicted
    );

    // Get a value (this will mark it as recently used)
    let value = lru.get(&2);
    println!("Get 2: {:?}", value);

    // Peek at a value (without marking as recently used)
    let value = lru.peek(&3);
    println!("Peek 3: {:?}", value);

    // Check if a key exists
    println!("Contains 1: {}", lru.contains(&1));
    println!("Contains 2: {}", lru.contains(&2));

    // Delete a key
    let (prev, deleted) = lru.delete(&2);
    println!("Delete 2: prev={:?}, deleted={}", prev, deleted);

    // Current length
    println!("Length: {}", lru.len());

    // Iterate from most to least recently used
    println!("Items from most to least recent:");
    lru.range(|k, v| {
        println!("  {}: {}", k, v);
        true
    });

    // Resize the cache
    let (evicted_keys, evicted_values) = lru.resize(2);
    println!(
        "Resized to 2, evicted: {:?}, {:?}",
        evicted_keys, evicted_values
    );

    // Clear the cache
    lru.clear();
    println!("After clear, length: {}", lru.len());
}
