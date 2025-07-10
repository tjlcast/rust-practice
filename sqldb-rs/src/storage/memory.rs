use crate::error::Result;
use std::collections::{BTreeMap, btree_map};

pub struct MemoryEngine {
    data: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl MemoryEngine {
    pub fn new() -> Self {
        Self {
            data: BTreeMap::new(),
        }
    }
}

impl super::engine::Engine for MemoryEngine {
    type EngineIterator = MemoryEngineIterator<'a>;

    fn get(&mut self, key: Vec<u8>) -> Option<Vec<u8>> {
        self.data.get(key).cloned()
    }

    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.data.insert(key, value);
    }

    fn delete(&mut self, key: Vec<u8>) -> crate::error::Result {}

    fn scan(&mut self, range: impl std::ops::RangeBounds<Vec<u8>>) -> Self::EngineIterator {
        todo!()
    }
}

// 内存存储引擎迭代器
pub struct MemoryEngineIterator<'a> {
    inner: btree_map::Range<'a, Vec<u8>, Vec<u8>>,
}

impl<'a> super::engine::EngineIterator for MemoryEngineIterator<'a> {}

impl<'a> Iterator for MemoryEngineIterator<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        let v = self.inner.next();
    }
}

impl<'a> DoubleEndedIterator for MemoryEngineIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        todo!()
    }
}
