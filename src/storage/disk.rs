use std::{
    collections::BTreeMap,
    io::{BufWriter, Read, Seek, SeekFrom, Write},
};

use crate::{error::Result, storage};

pub type KeyDir = BTreeMap<Vec<u8>, (u64, u32)>; // (offset, size)
const LOG_HEADER_SIZE: u32 = 8;

// 磁盘存储引擎定义
pub struct DiskEngine {
    keydir: KeyDir,
    log: Log,
}

impl storage::engine::Engine for DiskEngine {
    type EngineIterator<'a> = DiskEngineIterator;

    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        // 先写日志
        let (offset, size) = self.log.write_entry(&key, Some(&value))?;
        // 更新内存索引
        let val_size = value.len() as u32;
        self.keydir
            .insert(key, (offset + size as u64 - val_size as u64, val_size));
        Ok(())
    }

    fn get(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        match self.keydir.get(&key) {
            Some((offset, val_size)) => {
                let val = self.log.read_value(*offset, *val_size)?;
                Ok(Some(val))
            }
            None => Ok(None),
        }
    }

    fn delete(&mut self, key: Vec<u8>) -> Result<()> {
        self.log.write_entry(&key, None)?;
        self.keydir.remove(&key);
        Ok(())
    }

    fn scan(&mut self, range: impl std::ops::RangeBounds<Vec<u8>>) -> Self::EngineIterator<'_> {
        todo!()
    }
}

pub struct DiskEngineIterator {}

impl super::engine::EngineIterator for DiskEngineIterator {}

impl Iterator for DiskEngineIterator {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

impl DoubleEndedIterator for DiskEngineIterator {
    fn next_back(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

struct Log {
    file: std::fs::File,
}

impl Log {
    fn write_entry(&mut self, key: &Vec<u8>, value: Option<&Vec<u8>>) -> Result<(u64, u32)> {
        // 首先将文件偏移到末尾
        let offset = self.file.seek(SeekFrom::End(0))?;
        let key_size = key.len() as u32;
        let value_size = value.map_or(0, |v| v.len() as u32);
        let total_size = LOG_HEADER_SIZE + key_size + value_size;
        // 写入 key_size, value_size，key，value
        let mut writer = BufWriter::with_capacity(total_size as usize, &self.file);
        writer.write_all(&key_size.to_be_bytes())?;
        writer.write_all(&value.map_or(-1, |v| v.len() as i32).to_be_bytes())?;
        writer.write_all(&key)?;
        if let Some(value) = value {
            writer.write_all(value)?;
        }
        writer.flush()?;
        Ok((offset, total_size))
    }

    fn read_value(&mut self, offset: u64, val_size: u32) -> Result<Vec<u8>> {
        self.file.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0; val_size as usize];
        self.file.read_exact(&mut buf)?;
        Ok(buf)
    }
}
