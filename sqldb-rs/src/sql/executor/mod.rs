use schema::CreateTable;

use crate::{
    error::Result,
    sql::{
        engine::Transaction,
        executor::{mutation::Insert, query::Scan},
    },
};

use super::{plan::Node, types::Row};

mod mutation;
mod query;
mod schema;

// 执行器定义
pub trait Executor<T: Transaction> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet>;
}

// 执行结果集
#[derive(Debug)]
pub enum ResultSet {
    CreateTable { table_name: String },

    Insert { count: usize },

    Scan { columns: Vec<String>, rows: Vec<Row> },
}

impl<T: Transaction> dyn Executor<T> {
    // 把sql计划转化为sql执行器
    pub fn build(node: Node) -> Box<dyn Executor<T>> {
        match node {
            Node::CreateTable { schema } => CreateTable::new(schema),
            Node::Insert {
                table_name,
                columns,
                values,
            } => Insert::new(table_name, columns, values),
            Node::Scan { table_name } => Scan::new(table_name),
        }
    }
}
