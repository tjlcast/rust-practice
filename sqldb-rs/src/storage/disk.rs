use std::{
    collections::{BTreeMap, btree_map},
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Seek, Write},
    path::PathBuf,
};

// <key_binary, (file_value_binary_offset, val_binary_size)>
pub type KeyDir = BTreeMap<Vec<u8>, (u64, u32)>;

use fs4::FileExt;

use crate::{error::Result, storage::engine::EngineIterator};

const LOG_HEADER_SIZE: u32 = 8;

// 磁盘存储引擎定义
pub struct DiskEngine {
    keydir: KeyDir,
    // +-------------+-------------+----------------+----------------+​
    // | key len(4)    val len(4)     key(varint)       val(varint)  |​
    // +-------------+-------------+----------------+----------------+
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
        let mut eng: DiskEngine = Self::new(file_path)?;
        eng.compact()?;
        Ok(eng)
    }

    // 使用 keydir 的信息构建新的临时 keydir 和 log 文件
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
        std::fs::rename(&new_log.file_path, &self.log.file_path)?;

        // new_log.file_path = self.log.file_path;
        new_log.file_path = self.log.file_path.clone();
        self.keydir = new_keydir;
        self.log = new_log;

        Ok(())
    }
}

impl super::engine::Engine for DiskEngine {
    type EngineIterator<'a> = DiskEngineIterator<'a>;

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
        DiskEngineIterator {
            inner: self.keydir.range(range),
            log: &mut self.log,
        }
    }
}

pub struct DiskEngineIterator<'a> {
    // 这里的是 inner 是 keydir 的迭代器
    inner: btree_map::Range<'a, Vec<u8>, (u64, u32)>,
    log: &'a mut Log,
}

impl<'a> DiskEngineIterator<'a> {
    fn map(&mut self, item: (&Vec<u8>, &(u64, u32))) -> <Self as Iterator>::Item {
        let (k, (offset, val_size)) = item;
        let value: Vec<u8> = self.log.read_value(*offset, *val_size)?;
        Ok((k.clone(), value))
    }
}

impl<'a> EngineIterator for DiskEngineIterator<'a> {}

impl<'a> Iterator for DiskEngineIterator<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|item| self.map(item))
    }
}

impl<'a> DoubleEndedIterator for DiskEngineIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(|item| self.map(item))
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

    // 遍历数据文件，构建内存索引（并“删除”数据的过滤）
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
            // value_size == -1 means the key is deleted
            if val_size == -1 {
                keydir.remove(&key);
                offset += key_size as u64 + LOG_HEADER_SIZE as u64;
            } else {
                keydir.insert(
                    key,
                    (
                        offset + LOG_HEADER_SIZE as u64 + key_size as u64, // 这里存储的是 value 的偏移量
                        val_size as u32, // 这里存储的是 value 的大小
                    ),
                );
                offset += LOG_HEADER_SIZE as u64 + key_size as u64 + val_size as u64;
            }
        }

        Ok(keydir)
    }
}

impl Log {
    /// 在日志文件末尾追加一条记录。
    ///
    /// # 说明
    /// 1. 先把文件游标移动到文件末尾，得到当前偏移量 `offset`。
    /// 2. 计算 key 和 value（可为空）的字节长度，得到整条记录的总长度 `total_size`。
    /// 3. 按顺序写入：
    ///    - key 长度（u32，大端）
    ///    - value 长度（i32，大端；若 value 为 `None` 则写 `-1`）
    ///    - key 本身
    ///    - value（若存在）
    /// 4. 立即 flush，保证数据落盘。
    ///
    /// # 参数
    /// - `key`:   要写入的键，以 `&Vec<u8>` 形式传入。
    /// - `value`: 要写入的值，可为空（`Option<&Vec<u8>>`）。
    ///
    /// # 返回
    /// 成功时返回一个元组 `(offset, total_size)`：
    /// - `offset`: 该条记录在整个日志文件中的起始字节偏移量。
    /// - `total_size`: 该条记录占用的总字节数（包含头部）。
    ///
    fn write_entry(&mut self, key: &Vec<u8>, value: Option<&Vec<u8>>) -> Result<(u64, u32)> {
        // 首先把文件偏移移动到文件末尾
        let offset = self.file.seek(std::io::SeekFrom::End(0))?;
        let key_size = key.len() as u32;
        let val_size = value.map_or(0, |v| v.len() as u32);
        // 这里的 LOG_HEADER_SIZE 是 key_size 和 val_size 的二进制拼接
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

    /// Reads a value of specified size from a given offset in the file.
    ///
    /// # Arguments
    /// * `offset` - The position in the file (in bytes) from where to start reading.
    /// * `val_size` - The number of bytes to read.
    ///
    /// # Returns
    /// - `Ok(Vec<u8>)` containing the read bytes if successful
    /// - `Err` if either seeking to the offset or reading fails
    ///
    /// # Errors
    /// This function will return an error if:
    /// - The seek operation fails (invalid offset)
    /// - The read operation fails (not enough bytes available or other I/O error)
    /// - The file handle has been closed or is otherwise inaccessible
    ///
    fn read_value(&mut self, offset: u64, val_size: u32) -> Result<Vec<u8>> {
        self.file.seek(std::io::SeekFrom::Start(offset))?;
        let mut buf = vec![0; val_size as usize];
        self.file.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Reads a key-value entry from a buffered file reader at a specific offset.
    ///
    /// The entry is expected to be stored in the following binary format:
    /// 1. 4-byte big-endian key size (u32)
    /// 2. 4-byte big-endian value size (i32)
    /// 3. Key data (bytes)
    /// (Note: The actual value data is not read by this function)
    ///
    /// # Arguments
    /// * `buf_reader` - A buffered reader for the file containing the entries
    /// * `offset` - The byte offset in the file where the entry begins
    ///
    /// # Returns
    /// - `Ok((Vec<u8>, i32))` containing (key_bytes, value_size) if successful
    /// - `Err` if any I/O operation fails or if the data is malformed
    ///
    /// # Errors
    /// This function will return an error if:
    /// - Seeking to the specified offset fails
    /// - Reading either the key size or value size fails
    /// - Reading the key bytes fails
    /// - The file ends unexpectedly during reading
    ///
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
    use crate::storage::engine::Engine;

    fn cleanup_and_build_test_file(file_path_str: &str) -> Result<()> {
        let test_file_path = PathBuf::from(file_path_str);

        // 检查并处理tmp目录
        let tmp_dir = test_file_path.parent().unwrap();
        if tmp_dir.exists() {
            // 如果目录存在，清空目录中的文件
            if let Ok(entries) = std::fs::read_dir(tmp_dir) {
                for entry in entries {
                    if let Ok(entry) = entry {
                        let _ = std::fs::remove_file(entry.path());
                    }
                }
            }
        } else {
            // 如果目录不存在，创建目录
            std::fs::create_dir_all(tmp_dir)?;
        }

        Ok(())
    }

    #[test]
    fn test_disk_engine_compact() -> Result<()> {
        let test_file_name = "tmp/disk_engine";
        let test_file_path: PathBuf = PathBuf::from(test_file_name);

        cleanup_and_build_test_file(test_file_path.to_str().unwrap())?;

        let mut eng: DiskEngine = DiskEngine::new(test_file_path)?;

        // write some data
        let _ = eng.set(b"key1".to_vec(), b"value1".to_vec());
        let _ = eng.set(b"key2".to_vec(), b"value2".to_vec());
        let _ = eng.set(b"key3".to_vec(), b"value3".to_vec());

        eng.delete(b"key1".to_vec())?;
        eng.delete(b"key2".to_vec())?;

        // 重写
        let _ = eng.set(b"aa".to_vec(), b"value1".to_vec());
        let _ = eng.set(b"aa".to_vec(), b"value2".to_vec());
        let _ = eng.set(b"aa".to_vec(), b"value3".to_vec());
        let _ = eng.set(b"bb".to_vec(), b"value4".to_vec());
        let _ = eng.set(b"bb".to_vec(), b"value5".to_vec());

        let iter = eng.scan(..);
        let v = iter.collect::<Result<Vec<_>>>()?;
        assert_eq!(
            v,
            vec![
                (b"aa".to_vec(), b"value3".to_vec()),
                (b"bb".to_vec(), b"value5".to_vec()),
                (b"key3".to_vec(), b"value3".to_vec()),
            ]
        );
        drop(eng);

        // 重启测试
        let mut eng2 = DiskEngine::new_compact(PathBuf::from(test_file_name))?;
        let iter = eng2.scan(..);
        let v2 = iter.collect::<Result<Vec<_>>>()?;
        assert_eq!(
            v2,
            vec![
                (b"aa".to_vec(), b"value3".to_vec()),
                (b"bb".to_vec(), b"value5".to_vec()),
                (b"key3".to_vec(), b"value3".to_vec()),
            ]
        );
        drop(eng2);

        std::fs::remove_file(test_file_name)?;

        Ok(())
    }
}
