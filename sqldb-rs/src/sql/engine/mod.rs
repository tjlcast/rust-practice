pub mod kv;

use crate::{
    error::{Error, Result},
    sql::{
        executor::ResultSet,
        parser::{Parser, ast::Expression},
        plan::Plan,
        schema::Table,
        types::{Row, Value},
    },
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

impl<E: Engine + 'static> Session<E> {
    pub fn execute(&mut self, sql: &str) -> Result<ResultSet> {
        // SQL -- Parser --> STMT(AST) -- Planner --> Node(Plan)[data_schema, data_type] --> build_and_do_executor(in Node)
        match Parser::new(sql).parse()? {
            stmt => {
                let mut txn = self.engine.begin()?;
                // 这里 execute 方法是使用执行器的工厂方法利用刚构建的事务创建执行器，并执行
                // 执行器操作的数据视图是事务的视图(sqldb_rs::sql::engine::Transaction)
                match Plan::build(stmt)?.execute(&mut txn) {
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

    // 更新行
    fn update_row(&mut self, table: &Table, id: &Value, row: Row) -> Result<()>;

    // 删除行
    fn delete_row(&mut self, table: &Table, id: &Value) -> Result<()>;

    // 扫描表
    fn scan_table(
        &self,
        table_name: String,
        filter: Option<(String, Expression)>,
    ) -> Result<Vec<Row>>;

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
