mod kv;

use crate::{
    error::{Error, Result},
    sql::{executor::ResultSet, parser::Parser, plan::Plan, schema::Table, types::Row},
};

/*
通用SQL-Engine（抽象）
打开一个会话（固定），这个会话打开一个事务（抽象），执行SQL语句，提交事务，关闭会话
*/
pub trait Engine: Clone {
    type Transaction: Transaction;

    fn begin(&self) -> Result<Self::Transaction>;

    fn session(&self) -> Result<Session<Self>> {
        Ok(Session {
            engine: self.clone(),
        })
    }
}

// 客户端 session 定义
pub struct Session<E: Engine> {
    engine: E,
}

impl<E: Engine> Session<E> {
    pub fn execute(&mut self, sql: &str) -> Result<ResultSet> {
        match Parser::new(sql).parse()? {
            stmt => {
                let mut txn = self.engine.begin()?;

                match Plan::build(stmt).execute(&mut txn) {
                    Ok(result) => {
                        txn.commit()?;
                        Ok(result)
                    }
                    Err(err) => {
                        txn.rollback()?;
                        Err(err)
                    }
                }
            }
        }
    }
}

// 抽象的事务信息，包含了 DDL 和 DML 操作
// 底层可以接入普通的 KV 存储引擎，可以接入分布式存放引擎
pub trait Transaction {
    // 提交事务
    fn commit(&self) -> Result<()>;

    // 回滚事务
    fn rollback(&self) -> Result<()>;

    // 创建行
    fn create_row(&mut self, table_name: String, row: Row) -> Result<()>;

    // 扫描表
    fn scan_table(&self, table_name: String) -> Result<Vec<Row>>;

    // DDL 相关操作
    fn create_table(&mut self, table: Table) -> Result<()>;

    // 获取表信息
    fn get_table(&self, table_name: String) -> Result<Option<Table>>;

    // 获取表的信息，不存在则报错
    fn must_get_table(&self, table_name: String) -> Result<Table> {
        let t_table_name = table_name.clone();
        self.get_table(table_name)?.ok_or(Error::Internal(format!(
            "table {} does not exist",
            t_table_name
        )))
    }
}
