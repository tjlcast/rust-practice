use std::ops::{Bound, RangeBounds};

use crate::error::Result;

// 抽象存储引擎接口定义，接入不同的存储引擎，目前支持内存和简单的磁盘 KV 存储
pub trait Engine {
    type EngineIterator<'a>: EngineIterator
    where
        Self: 'a;

    // 设置 key/value
    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()>;

    // 获取 key 对应的数据
    fn get(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>>;

    // 删除 key 对应的数据, 如果不存在话则忽略
    fn delete(&mut self, key: Vec<u8>) -> Result<()>;

    // 扫描指定范围内的 key/value
    fn scan(&mut self, range: impl RangeBounds<Vec<u8>>) -> Self::EngineIterator<'_>;

    // 前缀扫描
    fn scan_prefix(&mut self, prefix: Vec<u8>) -> Self::EngineIterator<'_> {
        let start = Bound::Included(prefix.clone());
        let mut bound_prefix = prefix.clone();
        if let Some(last) = bound_prefix.iter_mut().last() {
            *last += 1;
        }
        let end = Bound::Excluded(bound_prefix);

        self.scan((start, end))
    }
}

// DoubleEndedIterator 是一个双向迭代器，可以向前和向后迭代
pub trait EngineIterator: DoubleEndedIterator<Item = Result<(Vec<u8>, Vec<u8>)>> {}
