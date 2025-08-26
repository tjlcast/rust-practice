use serde::Deserialize;
use serde::Serialize;

use crate::error::Error;
use crate::error::Result;
use crate::sql::engine::Engine;
use crate::sql::engine::Transaction;
use crate::sql::parser::ast::Expression;
use crate::sql::schema::Table;
use crate::sql::types::Row;
use crate::sql::types::Value;
use crate::storage::keycode_se::serialize_key;
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
        self.txn.commit()?;
        Ok(())
    }

    fn rollback(&self) -> Result<()> {
        self.txn.rollback()?;
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

        // 找到主键
        let primary_val = table.get_primary_key(&row)?;

        // 主键冲突检查
        let id_enc = Key::Row(table_name.clone(), primary_val.clone()).encode()?;
        // 如何主键冲突报错
        if self.txn.get(id_enc.clone())?.is_some() {
            return Err(Error::Internal(format!(
                "Duplicate data for primary key {} in table {}",
                primary_val, table_name
            )));
        }

        // 存储数据
        // let id = Key::Row(table_name.clone(), row[0].clone());
        let value = bincode::serialize(&row)?;
        self.txn.set(id_enc, value)?;

        Ok(())
    }

    fn update_row(&mut self, table: &Table, id: &Value, row: Row) -> Result<()> {
        let new_pk = table.get_primary_key(&row)?;
        // 更新了主键，则删除旧的数据
        if *id != new_pk {
            let key_enc = Key::Row(table.name.clone(), id.clone()).encode()?;
            self.txn.delete(key_enc)?;
        }

        let key_enc = Key::Row(table.name.clone(), new_pk).encode()?;
        let val_enc = bincode::serialize(&row)?;
        self.txn.set(key_enc, val_enc)?;
        Ok(())
    }

    fn delete_row(&mut self, table: &Table, id: &Value) -> Result<()> {
        let key_enc = Key::Row(table.name.clone(), id.clone()).encode()?;
        self.txn.delete(key_enc)?;
        Ok(())
    }

    fn scan_table(
        &self,
        table_name: String,
        filter: Option<(String, Expression)>,
    ) -> Result<Vec<Row>> {
        let table = self.must_get_table(table_name.clone())?;
        let prefix_enc = KeyPrefix::Row(table_name.clone()).encode()?;
        let results = self.txn.scan_prefix(prefix_enc)?;

        let mut rows = Vec::new();
        for result in results {
            // 过滤数据
            let row: Row = bincode::deserialize(&result.value)?;
            if let Some((col, expr)) = &filter {
                let col_index = table.get_col_index(&col)?;
                if Value::from_expression(expr.clone()) == row[col_index] {
                    rows.push(row);
                }
            } else {
                rows.push(row);
            }
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
        table.validate()?;

        let key_enc = Key::Table(table.name.clone()).encode()?;
        let value = bincode::serialize(&table)?;
        self.txn.set(key_enc, value)?;

        Ok(())
    }

    fn get_table(&self, table_name: String) -> Result<Option<Table>> {
        let key_enc = Key::Table(table_name).encode()?;
        let v = self
            .txn
            .get(key_enc)?
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

impl Key {
    pub fn encode(&self) -> Result<Vec<u8>> {
        serialize_key(self)
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum KeyPrefix {
    /// 假设我们使用某种前缀编码（如 bincode 或 protobuf），enum 的 tag 会被编码成一个整数，紧跟在后面的才是变体里携带的数据。
    /// 如果 Key 和 KeyPrefix 共用同一个 tag 0，那么：
    /// Key::Table("foo") → 0 | "foo"
    /// KeyPrefix::Table → 0 |（后面没有数据）
    /// 反序列化器在拿到前缀 0 后，发现后面没有数据，它既可能是“完整的 Key::Table（但数据缺失，报错）”，也可能是“KeyPrefix::Table”。二者无法区分。
    Table, // 对齐 枚举 Key，序列化占位 (Key::Table(s) 与 KeyPrefix::Table 在序列化后生成的字节前缀 必须不同，否则反序列化时无法区分“这是一个完整的 Key”还是“这是一个前缀”。)
    Row(String),
}

impl KeyPrefix {
    pub fn encode(&self) -> Result<Vec<u8>> {
        serialize_key(self)
    }
}

#[cfg(test)]
mod tests {
    use super::KVEngine;
    use crate::{
        error::{Error, Result},
        sql::{engine::Engine, executor::ResultSet, parser::ast::Column},
        storage::{disk::DiskEngine, memory::MemoryEngine},
    };

    fn setup_table<E: crate::storage::engine::Engine + 'static>(
        s: &mut crate::sql::engine::Session<KVEngine<E>>,
    ) -> Result<()> {
        s.execute(
            "create table t3 (
                a int primary key,
                b int default 12 null,
                c int default NULL,
                d float not null
            );",
        )?;

        s.execute(
            "create table t4 (
                a bool primary key,
                b int default 12,
                d boolean default true
            );",
        )?;

        Ok(())
    }

    #[test]
    fn test_create_table() -> Result<()> {
        let kv_engine = KVEngine::new(MemoryEngine::new());
        let mut session = kv_engine.session()?;

        session.execute("create table t1 (a int primary key, b text, c integer);")?;
        session.execute("insert into t1 values(1, 'a', 1);")?;

        let select_result = session.execute("select * from t1;")?;

        println!("select_result: {:?}", select_result);

        // 添加断言验证结果
        match &select_result {
            crate::sql::engine::ResultSet::Scan { columns, rows } => {
                assert_eq!(columns, &["a", "b", "c"]);
                assert_eq!(rows.len(), 1);
                let row = &rows[0];
                assert_eq!(row.len(), 3);
                assert_eq!(row[0], crate::sql::types::Value::Integer(1));
                assert_eq!(row[1], crate::sql::types::Value::String("a".to_string()));
                assert_eq!(row[2], crate::sql::types::Value::Integer(1));
            }
            _ => panic!("Expected Scan result, but got: {:?}", select_result),
        }

        // 构建期望的 ResultSet::Scan 进行比较
        let expected = crate::sql::engine::ResultSet::Scan {
            columns: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            rows: vec![vec![
                crate::sql::types::Value::Integer(1),
                crate::sql::types::Value::String("a".to_string()),
                crate::sql::types::Value::Integer(1),
            ]],
        };

        assert_eq!(select_result, expected);

        Ok(())
    }

    #[test]
    fn test_update() -> Result<()> {
        let kv_engine = KVEngine::new(MemoryEngine::new());
        let mut session = kv_engine.session()?;

        session.execute("create table t1 (a int primary key, b text, c integer);")?;
        session.execute("insert into t1 values(1, 'a', 1);")?;
        session.execute("insert into t1 values(2, 'b', 2);")?;
        session.execute("insert into t1 values(3, 'c', 3);")?;

        let result_set = session.execute("update t1 set b = 'aa' where a = 1;")?;
        println!("updated properties num: {:?}", result_set);
        assert_eq!(
            result_set,
            crate::sql::executor::ResultSet::Update { count: 1 }
        );

        let result_set = session.execute("select * from t1;")?;
        println!("select result after update properties: {:?}", result_set);
        let expected = crate::sql::engine::ResultSet::Scan {
            columns: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            rows: vec![
                vec![
                    crate::sql::types::Value::Integer(1),
                    crate::sql::types::Value::String("aa".to_string()),
                    crate::sql::types::Value::Integer(1),
                ],
                vec![
                    crate::sql::types::Value::Integer(2),
                    crate::sql::types::Value::String("b".to_string()),
                    crate::sql::types::Value::Integer(2),
                ],
                vec![
                    crate::sql::types::Value::Integer(3),
                    crate::sql::types::Value::String("c".to_string()),
                    crate::sql::types::Value::Integer(3),
                ],
            ],
        };
        assert_eq!(result_set, expected);

        let result_set = session.execute("update t1 set a = 33 where a = 3;")?;
        println!("result_set: {:?}", result_set);
        assert_eq!(
            result_set,
            crate::sql::executor::ResultSet::Update { count: 1 }
        );

        let result_set = session.execute("select * from t1;")?;
        println!("result_set: {:?}", result_set);
        let expected = crate::sql::engine::ResultSet::Scan {
            columns: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            rows: vec![
                vec![
                    crate::sql::types::Value::Integer(1),
                    crate::sql::types::Value::String("aa".to_string()),
                    crate::sql::types::Value::Integer(1),
                ],
                vec![
                    crate::sql::types::Value::Integer(2),
                    crate::sql::types::Value::String("b".to_string()),
                    crate::sql::types::Value::Integer(2),
                ],
                vec![
                    crate::sql::types::Value::Integer(33),
                    crate::sql::types::Value::String("c".to_string()),
                    crate::sql::types::Value::Integer(3),
                ],
            ],
        };
        assert_eq!(result_set, expected);

        Ok(())
    }

    #[test]
    fn test_delete() -> Result<()> {
        let kv_engine = KVEngine::new(MemoryEngine::new());
        let mut session = kv_engine.session()?;

        session.execute("create table t1 (a int primary key, b text, c integer);")?;
        session.execute("insert into t1 values(1, 'a', 1);")?;
        session.execute("insert into t1 values(2, 'b', 2);")?;
        session.execute("insert into t1 values(3, 'c', 3);")?;
        session.execute("delete from t1;")?;

        if let Ok(ResultSet::Scan { columns, rows }) = session.execute("select * from t1;") {
            assert_eq!(columns, vec!["a", "b", "c"]);
            assert_eq!(rows.len(), 0);
        } else {
            return Err(Error::Internal("invalid result set".to_string()));
        }

        session.execute("insert into t1 values(1, 'a', 1);")?;
        session.execute("insert into t1 values(2, 'b', 2);")?;
        session.execute("insert into t1 values(3, 'c', 3);")?;
        session.execute("delete from t1 where a = 2;")?;

        match session.execute("select * from t1;") {
            Ok(ResultSet::Scan { columns, rows }) => {
                assert_eq!(columns, vec!["a", "b", "c"]);
                assert_eq!(rows.len(), 2);
                Ok(())
            }
            _ => Err(Error::Internal("invalid result set".to_string())),
        }
    }

    #[test]
    fn test_order() -> Result<()> {
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
        let mut s = kvengine.session()?;
        setup_table(&mut s)?;

        s.execute("insert into t3 values (1, 34, 22, 1.22);")?;
        s.execute("insert into t3 values (4, 23, 65, 4.23);")?;
        s.execute("insert into t3 values (3, 56, 22, 2.88);")?;
        s.execute("insert into t3 values (2, 87, 57, 6.78);")?;
        s.execute("insert into t3 values (5, 87, 14, 3.28);")?;
        s.execute("insert into t3 values (7, 87, 82, 9.52);")?;

        match s.execute("select * from t3 order by d, c desc;")? {
            ResultSet::Scan { columns, rows } => {
                for r in rows {
                    println!("{:?}", r);
                }
            }
            _ => unreachable!(),
        }

        std::fs::remove_dir_all(p.parent().unwrap())?;

        Ok(())
    }

    #[test]
    fn test_select_limit_offset() -> Result<()> {
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
        let mut s = kvengine.session()?;
        setup_table(&mut s)?;

        s.execute("insert into t3 values (1, 34, 22, 1.22);")?;
        s.execute("insert into t3 values (4, 23, 65, 4.23);")?;
        s.execute("insert into t3 values (3, 56, 22, 2.88);")?;
        s.execute("insert into t3 values (2, 87, 57, 6.78);")?;
        s.execute("insert into t3 values (5, 87, 14, 3.28);")?;
        s.execute("insert into t3 values (7, 87, 82, 9.52);")?;

        match s.execute("select * from t3 order by a limit 3 offset 2;")? {
            ResultSet::Scan { columns, rows } => {
                for r in rows {
                    println!("{:?}", r);
                }
            }
            _ => unreachable!(),
        }

        std::fs::remove_dir_all(p.parent().unwrap())?;

        Ok(())
    }


    #[test]
    fn test_select_as() -> Result<()> {
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
        let mut s = kvengine.session()?;
        setup_table(&mut s)?;

        s.execute("insert into t3 values (1, 34, 22, 1.22);")?;
        s.execute("insert into t3 values (4, 23, 65, 4.23);")?;
        s.execute("insert into t3 values (3, 56, 22, 2.88);")?;
        s.execute("insert into t3 values (2, 87, 57, 6.78);")?;
        s.execute("insert into t3 values (5, 87, 14, 3.28);")?;
        s.execute("insert into t3 values (7, 87, 82, 9.52);")?;

        match s.execute("select a from t3 order by a limit 3 offset 2;")? {
            ResultSet::Scan { columns, rows } => {
                for col in &columns {
                    print!("{} ", col);
                }
                println!();
                for r in rows {
                    println!("{:?} ", r);
                }
            }
            _ => unreachable!(),
        }


        match s.execute("select a as aa, b as bb, c as cc, d as dd from t3 order by a limit 3 offset 2;")? {
            ResultSet::Scan { columns, rows } => {
                for col in &columns {
                    print!("{} ", col);
                }
                println!();
                for r in rows {
                    println!("{:?} ", r);
                }
            }
            _ => unreachable!(),
        }

        std::fs::remove_dir_all(p.parent().unwrap())?;

        Ok(())
    }
}
