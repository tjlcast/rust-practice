use schema::CreateTable;

use crate::{error::Result, sql::executor::query::Scan};

use super::{plan::Node, types::Row};

mod mutation;
mod query;
mod schema;

// 执行器定义
pub trait Executor {
    fn execute(&self) -> Result<ResultSet>;
}

impl dyn Executor {
    pub fn build(node: Node) -> Box<dyn Executor> {
        match node {
            Node::CreateTable { schema } => CreateTable::new(schema),
            Node::Insert {
                table_name,
                columns,
                values,
            } => todo!(),
            Node::Scan { table_name } => Scan::new(table_name),
        }
    }
}

// 执行结果集
pub enum ResultSet {
    CreateTable { table_name: String },

    Insert { count: usize },

    Scan { columns: Vec<String>, row: Vec<Row> },
}
