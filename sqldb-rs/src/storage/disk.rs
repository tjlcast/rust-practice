use std::{
    collections::BTreeMap,
    env,
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Seek, Write},
    path::PathBuf,
};

pub type KeyDir = BTreeMap<Vec<u8>, (u64, u32)>;

use fs4::FileExt;

use crate::{error::Result, storage::engine::EngineIterator};

const LOG_HEADER_SIZE: u32 = 8;

pub struct DiskEngine {
    keydir: KeyDir,
    log: Log,
}

impl DiskEngine {
    pub fn new(file_path: PathBuf) -> Result<Self> {
        let mut log = Log::new(file_path)?;
        // 从 log 中去恢复的 keydir
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
        for (key, (offset, val_size)) in self.keydir.iter() {
            // 读取 value
            let value = self.log.read_value(*offset, *val_size)?;
            // 写入新的临时log文件中
            let (new_offset, new_size) = new_log.write_entry(key, Some(&value))?;
            // 写入新的 keydir 中
            // new_keydir.insert(
            //     key,
            //     (new_offset + new_size as u64 - val_size as u64, val_size),
            // );
            new_keydir.insert(
                key.clone(),
                (new_offset + new_size as u64 - *val_size as u64, *val_size),
            );
        }

        // 将临时文件更改为正式文件
        // std::fs::rename(new_log.file_path, self.log.file_path);
        std::fs::rename(&new_log.file_path, &self.log.file_path);

        // new_log.file_path = self.log.file_path;
        new_log.file_path = self.log.file_path.clone();
        self.keydir = new_keydir;
        self.log = new_log;

        Ok(())
    }
}

impl super::engine::Engine for DiskEngine {
    type EngineIterator<'a> = DiskEngineIterator;

    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        // 先写日志记录
        let (offset, size) = self.log.write_entry(&key, Some(&value))?;
        // 更新内存索引
        let val_size = value.len() as u32;
        // keydir 中的value表示数据value的偏移量
        self.keydir
            .insert(key, (offset + size as u64 - val_size as u64, val_size));

        Ok(())
    }

    fn get(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        match self.keydir.get(&key) {
            Some((offset, val_size)) => {
                /*
                    自动解引用的核心机制
                    Rust 会在以下情况自动解引用：
                        T: Deref<Target = U> 时，&T 可以自动转为 &U
                        例如：&String 可以自动转为 &str（因为 String 实现了 Deref<Target = str>）。

                        T: Copy 时，* 解引用本质上是进行复制赋值（如果没有实现 Copy trait，则只能通过引用进行赋值; 如果使用 *T，编译器会报错）。

                */
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

impl EngineIterator for DiskEngineIterator {}

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
    file_path: PathBuf,
    file: std::fs::File,
}

impl Log {
    fn new(file_path: PathBuf) -> Result<Self> {
        // 如果文件不存在，则创建
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
        // 使用第三库 fs4
        file.try_lock_exclusive()?;

        Ok(Self { file, file_path })
    }

    // 遍历数据文件，构建内存索引
    fn build_keydir(&mut self) -> Result<KeyDir> {
        let mut keydir = KeyDir::new();
        let file_size = self.file.metadata()?.len();
        let mut buf_reader: BufReader<&File> = BufReader::new(&self.file);

        let mut offset = 0;
        loop {
            if offset >= file_size {
                break;
            }

            let (key, val_size) = Self::read_entry(&mut buf_reader, offset)?;
            let key_size = key.len() as u32;
            if val_size == -1 {
                keydir.remove(&key);
                offset += key_size as u64 + LOG_HEADER_SIZE as u64;
            } else {
                keydir.insert(
                    key,
                    (
                        offset + (LOG_HEADER_SIZE + key_size) as u64, // 这里存储的是 value 的偏移量
                        val_size as u32,                              // 这里存储的是 value 的大小
                    ),
                );
                offset += LOG_HEADER_SIZE as u64 + key_size as u64 + val_size as u64;
            }
        }

        Ok(keydir)
    }
}

impl Log {
    fn write_entry(&mut self, key: &Vec<u8>, value: Option<&Vec<u8>>) -> Result<(u64, u32)> {
        // 首先把文件偏移移动到文件末尾
        let offset = self.file.seek(std::io::SeekFrom::End(0))?;
        let key_size = key.len() as u32;
        let val_size = value.map_or(0, |v| v.len()) as u32;
        let total_size = LOG_HEADER_SIZE + key_size + val_size;

        // 分别写入 key size, value size, key, value
        let mut writer = BufWriter::with_capacity(total_size as usize, &self.file);
        writer.write_all(&key_size.to_be_bytes())?;
        writer.write_all(&value.map_or(-1, |v| v.len() as i32).to_be_bytes())?;
        writer.write_all(&key)?;
        if let Some(val) = value {
            writer.write_all(val)?;
        }
        writer.flush()?;

        Ok((offset, total_size))
    }

    fn read_value(&mut self, offset: u64, val_size: u32) -> Result<Vec<u8>> {
        self.file.seek(std::io::SeekFrom::Start(offset))?;
        let mut buf = vec![0; val_size as usize];
        self.file.read_exact(&mut buf)?;
        Ok(buf)
    }

    fn read_entry(buf_reader: &mut BufReader<&File>, offset: u64) -> Result<(Vec<u8>, i32)> {
        buf_reader.seek(std::io::SeekFrom::Start(offset))?;
        let mut len_buf = [0; 4];

        // 读取 key size
        buf_reader.read_exact(&mut len_buf)?;
        let key_size = u32::from_be_bytes(len_buf);

        // 读取 value size
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
    let temp_dir = env::temp_dir();
    let db_path = temp_dir.join("rocksdb");
    println!("path: {:?}", db_path);
    DiskEngine::new(db_path)?;
    Ok(())
}
