use serde::Deserialize;
use serde::Serialize;

use crate::error::Error;
use crate::error::Result;
use crate::sql::engine::Engine;
use crate::sql::engine::Transaction;
use crate::sql::schema::Table;
use crate::sql::types::Row;
use crate::sql::types::Value;
use crate::storage::{self, engine::Engine as StorageEngine};

pub struct KVEngine<E: StorageEngine> {
    pub storage_mvcc: storage::mvcc::Mvcc<E>,
}

impl<E: StorageEngine> KVEngine<E> {
    pub fn new(engine: E) -> Self {
        Self {
            storage_mvcc: storage::mvcc::Mvcc::new(engine),
        }
    }
}

impl<E: StorageEngine> Clone for KVEngine<E> {
    fn clone(&self) -> Self {
        Self {
            storage_mvcc: self.storage_mvcc.clone(),
        }
    }
}

impl<E: StorageEngine> Engine for KVEngine<E> {
    type Transaction = KVTransaction<E>;

    fn begin(&self) -> Result<Self::Transaction> {
        Ok(Self::Transaction::new(self.storage_mvcc.begin()?))
    }
}

pub struct KVTransaction<E: StorageEngine> {
    txn: storage::mvcc::MvccTransaction<E>,
}

impl<E: StorageEngine> KVTransaction<E> {
    pub fn new(txn: storage::mvcc::MvccTransaction<E>) -> Self {
        Self { txn }
    }
}

impl<E: StorageEngine> Transaction for KVTransaction<E> {
    fn commit(&self) -> Result<()> {
        Ok(())
    }

    fn rollback(&self) -> Result<()> {
        Ok(())
    }

    fn create_row(&mut self, table_name: String, row: Row) -> Result<()> {
        let table = self.must_get_table(table_name.clone())?;
        // 校验行的有效性
        for (i, col) in table.columns.iter().enumerate() {
            match row[i].datatype() {
                None if col.nullable => {}
                None => {
                    return Err(Error::Internal(format!(
                        "column {} is not nullable",
                        col.name
                    )));
                }
                Some(dt) if dt != col.datatype => {
                    return Err(Error::Internal(format!(
                        "column {} has wrong type",
                        col.name
                    )));
                }
                _ => {}
            }
        }

        // 存储数据
        // 暂时以第一列作为主键，一行数据的唯一标志, todo
        let id = Key::Row(table_name.clone(), row[0].clone());
        let value = bincode::serialize(&row)?;
        self.txn.set(bincode::serialize(&id)?, value)?;

        Ok(())
    }

    fn scan_table(&self, table_name: String) -> Result<Vec<Row>> {
        let mut rows = Vec::new();
        let prefix = KeyPrefix::Row(table_name.clone());
        let results = self.txn.scan_prefix(bincode::serialize(&prefix)?)?;
        for result in results {
            let row: Row = bincode::deserialize(&result.value)?;
            rows.push(row);
        }
        Ok(rows)
    }

    fn create_table(&mut self, table: Table) -> Result<()> {
        // 判断表是否存在
        if self.get_table(table.name.clone())?.is_some() {
            return Err(Error::Internal(format!(
                "table {} already exists.",
                table.name
            )));
        }

        // 判断表是否有效
        if table.columns.is_empty() {
            return Err(Error::Internal(format!(
                "table {} has no columns.",
                table.name
            )));
        }

        let key = Key::Table(table.name.clone());
        let value = bincode::serialize(&table)?;
        self.txn.set(bincode::serialize(&key)?, value)?;

        Ok(())
    }

    fn get_table(&self, table_name: String) -> Result<Option<Table>> {
        let key = Key::Table(table_name);
        let v = self
            .txn
            .get(bincode::serialize(&key)?)?
            .map(|bytes| bincode::deserialize(&bytes))
            .transpose()?;
        Ok(v)
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum Key {
    Table(String),
    Row(String, Value),
}

#[derive(Debug, Serialize, Deserialize)]
enum KeyPrefix {
    Table, // 对齐 枚举 Key，序列化占位
    Row(String),
}

#[cfg(test)]
mod tests {
    use super::KVEngine;
    use crate::{error::Result, sql::engine::Engine, storage::memory::MemoryEngine};

    #[test]
    fn test_create_table() -> Result<()> {
        let kv_engine = KVEngine::new(MemoryEngine::new());
        let mut session = kv_engine.session()?;

        session.execute("create table t1 (a int, b text, c integer);")?;
        session.execute("insert into t1 values(1, 'a', 1);")?;

        let select_result = session.execute("select * from t1;")?;

        println!("select_result: {:?}", select_result);
        Ok(())
    }
}
