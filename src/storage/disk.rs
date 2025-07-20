use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::PathBuf,
};

use fs4::FileExt;

use crate::{error::Result, storage};

pub type KeyDir = BTreeMap<Vec<u8>, (u64, u32)>; // (offset, size)
const LOG_HEADER_SIZE: u32 = 8;

// 磁盘存储引擎定义
pub struct DiskEngine {
    keydir: KeyDir,
    log: Log,
}

impl DiskEngine {
    pub fn new(file_path: PathBuf) -> Result<Self> {
        let log = Log::new(file_path)?;
        // 从 log 中加载 keydir
        let keydir = log.build_keydir()?;
        Ok(Self { keydir, log })
    }

    pub fn new_compact(file_path: PathBuf) -> Result<Self> {
        let mut eng = Self::new(file_path)?;
        eng.compact()?;
        Ok(eng)
    }

    fn compact(&mut self) -> Result<()> {
        // 新打开一个临时日志文件
        let mut new_path = self.log.file_path.clone();
        new_path.set_extension("compact");

        let mut new_log = Log::new(new_path)?;
        let mut new_keydir = KeyDir::new();
        // 重写数据到临时文件中
        for (key, (offset, val_size)) in &self.keydir {
            // 读取 value
            let value = self.log.read_value(*offset, *val_size)?;
            let (new_offset, new_size) = new_log.write_entry(key, Some(&value))?;
            new_keydir.insert(
                key.clone(),
                (new_offset + new_size as u64 - *val_size as u64, *val_size),
            );
        }
        // 重命名临时文件为正式日志文件
        std::fs::rename(&new_log.file_path, &self.log.file_path)?;
        new_log.file_path = self.log.file_path.clone();
        self.keydir = new_keydir;
        self.log = new_log;
        Ok(())
    }
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
    file_path: PathBuf,
}

impl Log {
    pub fn new(file_path: PathBuf) -> Result<Self> {
        // 如果目录不存在的话则创建
        if let Some(dir) = file_path.parent() {
            if !dir.exists() {
                std::fs::create_dir_all(&dir)?;
            }
        }
        // 打开文件
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&file_path)?;
        // 加文件锁，保证同时只能有一个服务去使用这个文件
        file.try_lock_exclusive()?;
        Ok(Self { file, file_path })
    }

    // 遍历数据文件，构建内存索引
    fn build_keydir(&self) -> Result<KeyDir> {
        let mut keydir = KeyDir::new();
        let file_size = self.file.metadata()?.len();
        let mut buf_reader = BufReader::new(&self.file);
        let mut offset = 0;
        loop {
            if offset >= file_size {
                break;
            }
            let (key, val_size) = Self::read_entry(&mut buf_reader, offset)?;
            let key_size = key.len() as u32;
            if val_size == -1 {
                keydir.remove(&key);
                offset += LOG_HEADER_SIZE as u64 + key_size as u64;
            } else {
                keydir.insert(
                    key,
                    (
                        offset + LOG_HEADER_SIZE as u64 + key_size as u64,
                        val_size as u32,
                    ),
                );
                offset += LOG_HEADER_SIZE as u64 + key_size as u64 + val_size as u64;
            }
        }
        Ok(keydir)
    }

    // +-------------+-------------+----------------+----------------+
    // | key len(4)    val len(4)     key(varint)       val(varint)  |
    // +-------------+-------------+----------------+----------------+
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

    fn read_entry(buf_reader: &mut BufReader<&File>, offset: u64) -> Result<(Vec<u8>, i32)> {
        buf_reader.seek(SeekFrom::Start(offset))?;
        let mut len_buf = [0; 4];
        // 读取 key_size
        buf_reader.read_exact(&mut len_buf)?;
        let key_size = u32::from_be_bytes(len_buf);
        // 读取 value_size
        buf_reader.read_exact(&mut len_buf)?;
        let val_size = i32::from_be_bytes(len_buf);
        // 读取 key
        let mut key = vec![0; key_size as usize];
        buf_reader.read_exact(&mut key)?;
        Ok((key, val_size))
    }
}

#[test]
fn test_disk_engine_start() -> Result<()> {
    let eng = DiskEngine::new_compact(PathBuf::from("/tmp/rsdb-log"))?;
    Ok(())
}
