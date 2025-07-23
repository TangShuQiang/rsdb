use std::{
    collections::HashSet,
    sync::{Arc, Mutex, MutexGuard},
    u64, vec,
};

use serde::{Deserialize, Serialize};

use crate::{
    error::{Error, Result},
    storage::engine::Engine,
};

type Version = u64;

pub struct Mvcc<E: Engine> {
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
        MvccTransaction::begin(self.engine.clone())
    }
}

pub struct MvccTransaction<E: Engine> {
    engine: Arc<Mutex<E>>,
    state: TransactionState, // 事务状态：当前事务的版本号和活跃事务列表
}

impl<E: Engine> MvccTransaction<E> {
    // 开启事务
    pub fn begin(eng: Arc<Mutex<E>>) -> Result<Self> {
        // 获取存储引擎
        let mut engine = eng.lock()?;
        // 获取最新的版本号
        let next_version = match engine.get(MvccKey::NextVersion.encode())? {
            Some(value) => bincode::deserialize(&value)?,
            None => 0,
        };
        // 保存下一个 version
        engine.set(
            MvccKey::NextVersion.encode(),
            bincode::serialize(&(next_version + 1))?,
        )?;
        // 获取当前活跃事务版本列表
        let active_versions = Self::scan_active(&mut engine)?;
        // 将当前事务加入到的活跃事务列表中
        engine.set(MvccKey::TxnActive(next_version).encode(), vec![])?;
        Ok(Self {
            engine: eng.clone(),
            state: TransactionState {
                version: next_version,
                active_versions,
            },
        })
    }

    pub fn commit(&self) -> Result<()> {
        let mut engine = self.engine.lock()?;
        let mut txnwrite_keys = Vec::new();
        // 找到当前事务的 TxnWrite 信息
        let mut iter = engine.scan_prefix(MvccKeyPrefix::TxnWrite(self.state.version).encode());
        while let Some((key, _)) = iter.next().transpose()? {
            txnwrite_keys.push(key);
        }
        drop(iter); // 释放迭代器锁
        // 删除当前事务的写入记录
        for key in txnwrite_keys {
            engine.delete(key)?;
        }
        // 删除当前事务的活跃状态
        engine.delete(MvccKey::TxnActive(self.state.version).encode())
    }

    pub fn rollback(&self) -> Result<()> {
        let mut engine = self.engine.lock()?;
        let mut txnwrite_keys = Vec::new();
        let mut version_keys = Vec::new();
        let mut iter = engine.scan_prefix(MvccKeyPrefix::TxnWrite(self.state.version).encode());
        while let Some((key, _)) = iter.next().transpose()? {
            txnwrite_keys.push(key.clone());
            match MvccKey::decode(key.clone())? {
                MvccKey::TxnWrite(_, raw_key) => {
                    version_keys.push(MvccKey::Version(raw_key, self.state.version).encode());
                }
                _ => {
                    return Err(Error::Internal(format!(
                        "unexpected key: {:?}",
                        String::from_utf8(key)
                    )));
                }
            }
        }
        drop(iter);
        // 删除当前事务的写入记录
        for key in txnwrite_keys {
            engine.delete(key)?;
        }
        // 删除当前事务的版本记录
        for key in version_keys {
            engine.delete(key)?;
        }
        engine.delete(MvccKey::TxnActive(self.state.version).encode())
    }

    pub fn set(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.write_inner(key, Some(value))
    }

    pub fn delete(&self, key: Vec<u8>) -> Result<()> {
        self.write_inner(key, None)
    }

    pub fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let mut engine = self.engine.lock()?;
        let from = MvccKey::Version(key.clone(), self.state.version).encode();
        let to = MvccKey::Version(key.clone(), self.state.version).encode();
        let mut iter = engine.scan(from..to).rev();
        // 从最新的版本开始查找，找到第一个可见的版本
        while let Some((key, value)) = iter.next().transpose()? {
            match MvccKey::decode(key.clone())? {
                MvccKey::Version(_, version) => {
                    if self.state.is_visible(version) {
                        return Ok(bincode::deserialize(&value)?);
                    }
                }
                _ => {
                    return Err(Error::Internal(format!(
                        "unexpected key: {:?}",
                        String::from_utf8(key)
                    )));
                }
            }
        }
        Ok(None)
    }

    pub fn scan_prefix(&self, prefix: Vec<u8>) -> Result<Vec<ScanResult>> {
        let mut eng = self.engine.lock()?;
        let mut iter = eng.scan_prefix(prefix);
        let mut results = Vec::new();
        while let Some((key, value)) = iter.next().transpose()? {
            results.push(ScanResult { key, value });
        }
        Ok(results)
    }

    // 更新 / 删除 数据
    fn write_inner(&self, key: Vec<u8>, value: Option<Vec<u8>>) -> Result<()> {
        // 获取存储引擎
        let mut engine = self.engine.lock()?;
        // 检测冲突
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
        if let Some((k, _)) = engine.scan(from..to).last().transpose()? {
            match MvccKey::decode(k.clone())? {
                MvccKey::Version(_, version) => {
                    // 检测这个 version 是否可见
                    if !self.state.is_visible(version) {
                        return Err(Error::WriteConflict);
                    }
                }
                _ => {
                    return Err(Error::Internal(format!(
                        "unexpected key: {:?}",
                        String::from_utf8(k)
                    )));
                }
            }
        }
        // 记录这个 version 写入了哪些key，用于回滚事务
        engine.set(
            MvccKey::TxnWrite(self.state.version, key.clone()).encode(),
            vec![],
        )?;
        // 写入数据
        engine.set(
            MvccKey::Version(key.clone(), self.state.version).encode(),
            bincode::serialize(&value)?,
        )?;
        Ok(())
    }

    // 扫描获取当前活跃事务列表
    fn scan_active(engine: &mut MutexGuard<E>) -> Result<HashSet<Version>> {
        let mut active_versions = HashSet::new();
        let mut iter = engine.scan_prefix(MvccKeyPrefix::TxnActive.encode());
        // 这个 key 是 MvccKey::TxnActive(version)
        while let Some((key, _)) = iter.next().transpose()? {
            match MvccKey::decode(key.clone())? {
                MvccKey::TxnActive(version) => {
                    active_versions.insert(version);
                }
                _ => {
                    return Err(Error::Internal(format!(
                        "unexpected key: {:?}",
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

// 事务状态
pub struct TransactionState {
    // 当前事务的版本号
    pub version: Version,
    // 当前活跃事务版本列表
    pub active_versions: HashSet<Version>,
}

impl TransactionState {
    fn is_visible(&self, version: Version) -> bool {
        if self.active_versions.contains(&version) {
            return false; // 如果版本在活跃事务列表中，则不可见
        }
        return version < self.version; // 如果版本小于当前事务版本，则可见
    }
}

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
