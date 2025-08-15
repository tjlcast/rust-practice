use std::{
    collections::HashSet,
    sync::{Arc, Mutex, MutexGuard},
};

use serde::{Deserialize, Serialize};

use super::engine::Engine;
use crate::error::{Error, Result};

pub type Version = u64;

pub struct Mvcc<E: Engine> {
    // 这里是 storage_engine
    engine: Arc<Mutex<E>>,
}

impl<E: Engine> Clone for Mvcc<E> {
    fn clone(&self) -> Self {
        Self {
            engine: self.engine.clone(),
        }
    }
}

impl<E: Engine> Mvcc<E> {
    pub fn new(eng: E) -> Self {
        Self {
            engine: Arc::new(Mutex::new(eng)),
        }
    }

    pub fn begin(&self) -> Result<MvccTransaction<E>> {
        // Ok(MvccTransaction::begin(self.engine.clone()))
        MvccTransaction::begin(self.engine.clone())
    }
}

pub struct MvccTransaction<E: Engine> {
    engine: Arc<Mutex<E>>,
    state: TransactionState, // 事务状态
}

pub struct TransactionState {
    // 当前事务的版本号
    pub version: Version,
    // 当前活跃事务的版本列表
    pub active_versions: HashSet<Version>,
}

impl TransactionState {
    fn is_visible(&self, version: Version) -> bool {
        if self.active_versions.contains(&version) {
            return false;
        } else {
            return version <= self.version;
        }
    }
}

// NextVersion: 0
// TxnActive: 1[100] 1[101] 1[102]
#[derive(Debug, Serialize, Deserialize)]
pub enum MvccKey {
    NextVersion,
    TxnActive(Version),
    TxnWrite(Version, Vec<u8>),
    Version(Vec<u8>, Version),
}

impl MvccKey {
    pub fn encode(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }

    pub fn decode(data: Vec<u8>) -> Result<Self> {
        Ok(bincode::deserialize(&data)?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum MvccKeyPrefix {
    NextVersion,
    TxnActive,
    TxnWrite(Version),
}

impl MvccKeyPrefix {
    pub fn encode(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }
}

impl<E: Engine> MvccTransaction<E> {
    // 开启事务
    pub fn begin(eng: Arc<Mutex<E>>) -> Result<Self> {
        // Self { engine: eng }

        // 获取存储引擎
        let mut storage_engine = eng.lock()?;
        //  获取最新的版本号
        let next_version = match storage_engine.get(MvccKey::NextVersion.encode())? {
            Some(value) => bincode::deserialize(&value)?,
            None => 1,
        };
        // 保存下一个version
        storage_engine.set(
            MvccKey::NextVersion.encode(),
            bincode::serialize(&(next_version + 1))?,
        )?;

        // 获取当前活跃的事务列表
        let active_versions = Self::scan_active(&mut storage_engine)?;

        // 当前事务加入到活跃事务列表中
        storage_engine.set(MvccKey::TxnActive(next_version).encode(), vec![])?;

        // 返回事务对象
        Ok(Self {
            engine: eng.clone(),
            state: TransactionState {
                version: next_version,
                active_versions: active_versions,
            },
        })
    }

    // 提交事务
    pub fn commit(&self) -> Result<()> {
        // Ok(())

        // 获取存储引擎
        let mut storage_engine = self.engine.lock()?;

        let mut delete_keys = Vec::new();
        // 找到这个当前事务的 TxnWrite 信息
        let mut iter =
            storage_engine.scan_prefix(MvccKeyPrefix::TxnWrite(self.state.version).encode());
        while let Some((key, _)) = iter.next().transpose()? {
            delete_keys.push(key);
        }
        drop(iter); // iter 内部持有了对 storage_engine 的引用，所以需要提前 drop，否则 storage_engine 的可变引用与下面的 storeage_engine.delete 冲突

        for key in delete_keys.into_iter() {
            storage_engine.delete(key)?;
        }

        // 从活跃事务列表中删除
        storage_engine.delete(MvccKey::TxnActive(self.state.version).encode())?;

        Ok(())
    }

    // 回滚事务
    pub fn rollback(&self) -> Result<()> {
        // Ok(())

        // 获取存储引擎
        let mut storage_engine = self.engine.lock()?;

        let mut delete_keys = Vec::new();
        // 找到这个当前事务的 TxnWrite 信息
        let mut iter =
            storage_engine.scan_prefix(MvccKeyPrefix::TxnWrite(self.state.version).encode());
        while let Some((key, _)) = iter.next().transpose()? {
            // 添加回溯的增量
            match MvccKey::decode(key.clone())? {
                MvccKey::TxnWrite(_, raw_key) => {
                    delete_keys.push(MvccKey::Version(raw_key, self.state.version).encode());
                }
                _ => {
                    return Err(Error::Internal(format!(
                        "Invalid key: {:?}",
                        String::from_utf8(key)
                    )));
                }
            }
            // 把本事务的记录信息删除
            delete_keys.push(key);
        }
        drop(iter); // iter 内部持有了对 storage_engine 的引用，所以需要提前 drop，否则 storage_engine 的可变引用与下面的 storeage_engine.delete 冲突

        for key in delete_keys.into_iter() {
            storage_engine.delete(key)?;
        }

        // 从活跃事务列表中删除
        storage_engine.delete(MvccKey::TxnActive(self.state.version).encode())?;

        Ok(())
    }

    pub fn set(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        // let mut storage_engine = self.engine.lock()?;
        // storage_engine.set(key, value)
        self.write_inner(key, Some(value))
    }

    pub fn delete(&self, key: Vec<u8>) -> Result<()> {
        self.write_inner(key, None)
    }

    pub fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        // let mut storage_engine = self.engine.lock()?;
        // storage_engine.get(key)

        let mut storage_engine = self.engine.lock()?;
        // version: 9
        // 扫描的 version 的范围应该是 0-9

        // 获取存储引擎
        let from = MvccKey::Version(key.clone(), 0).encode();
        let to = MvccKey::Version(key.clone(), self.state.version).encode();
        let mut iter = storage_engine.scan(from..=to).rev();
        // 从最新的版本开始读取，找到一个最新的可见版本
        while let Some((key, value)) = iter.next().transpose()? {
            match MvccKey::decode(key.clone())? {
                MvccKey::Version(_, version) => {
                    if self.state.is_visible(version) {
                        return Ok(bincode::deserialize(&value)?);
                    }
                }
                _ => {
                    return Err(Error::Internal(format!(
                        "Unexpected key: {:?}",
                        String::from_utf8(key)
                    )));
                }
            }
        }

        Ok(None)
    }

    pub fn scan_prefix(&self, prefix: Vec<u8>) -> Result<Vec<ScanResult>> {
        let mut storage_engine = self.engine.lock()?;
        let mut iter = storage_engine.scan_prefix(prefix);
        let mut results = Vec::new();
        while let Some((key, value)) = iter.next().transpose()? {
            results.push(ScanResult { key, value });
        }
        Ok(results)
    }

    // 更新/删除数据
    /// 构造扫描范围：从当前活跃事务的最小版本号到最大版本号（u64::MAX）
    /// 目的是检查在本次事务开始后，是否有其他事务修改了同一个key
    /// 扫描指定范围内的所有版本
    /// 取最后一个版本（因为版本号是递增的，最后一个是最新的）
    /// 检查该版本是否对当前事务可见：
    ///     如果版本属于活跃事务（未提交），则不可见 → 写冲突
    ///     如果版本号大于当前事务版本号 → 写冲突
    /// 记录当前事务修改了哪些key，以便在回滚时能够找到并撤销这些修改
    /// 以 Version(key, version) 的形式存储数据
    /// 如果是删除操作，value会被序列化为None
    fn write_inner(&self, key: Vec<u8>, value: Option<Vec<u8>>) -> Result<()> {
        // 获取存储引擎
        let mut storage_engine = self.engine.lock()?;

        // 检查冲突
        // 3 4 5
        // 6
        // key1-3 key2-4 key3-5
        let from = MvccKey::Version(
            key.clone(),
            self.state
                .active_versions
                .iter()
                .min()
                .copied()
                .unwrap_or(self.state.version + 1),
        )
        .encode();
        let to = MvccKey::Version(key.clone(), u64::MAX).encode();
        // 当前活跃事务列表 3 4 5
        // 当前事务 6
        // 只需要判断最后一个版本号
        // 1\ key 按照顺序排列，扫描出的结果是从小到大的
        // 2\ 假如有的事务修改了这个 key，比如 10，那么 6 再修改就是冲突的
        // 3\ 如果是当前活跃事务修改了这个 key，比如 4，那么事务 5 就不可能修改这个 key
        if let Some((k, _)) = storage_engine.scan(from..=to).last().transpose()? {
            match MvccKey::decode(k.clone()) {
                Ok(MvccKey::Version(_, version)) => {
                    // 检测这个 version 是否可见的
                    if !self.state.is_visible(version) {
                        return Err(Error::WriteConflict);
                    }
                }
                _ => {
                    return Err(Error::Internal(format!(
                        "Unexpected key: {:?}",
                        String::from_utf8(k)
                    )));
                }
            }
        }

        // 记录这个 version 写入了哪些 key， 用于回滚事务
        storage_engine.set(
            MvccKey::TxnWrite(self.state.version, key.clone()).encode(),
            vec![],
        )?;

        // 写入实际的 key/value 数据
        storage_engine.set(
            MvccKey::Version(key.clone(), self.state.version).encode(),
            bincode::serialize(&value)?,
        )?;

        Ok(())
    }

    // 扫描获取指定活跃的事务列表
    fn scan_active(engine: &mut MutexGuard<E>) -> Result<HashSet<Version>> {
        let mut active_versions = HashSet::new();
        let mut iter = engine.scan_prefix(MvccKeyPrefix::TxnActive.encode());
        while let Some((key, _)) = iter.next().transpose()? {
            match MvccKey::decode(key.clone())? {
                MvccKey::TxnActive(version) => {
                    active_versions.insert(version);
                }
                _ => {
                    return Err(Error::Internal(format!(
                        "Unexpected key: {:?}",
                        String::from_utf8(key)
                    )));
                }
            }
        }
        Ok(active_versions)
    }
}

pub struct ScanResult {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}
