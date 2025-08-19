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
#[derive(Debug, PartialEq)]
pub enum ResultSet {
    CreateTable { table_name: String },

    Insert { count: usize },

    Scan { columns: Vec<String>, rows: Vec<Row> },
}

///
/// 为什么写成 impl<T: Transaction> dyn Executor<T> { ... }？
/// 不是给某个具体类型实现 trait，而是给trait object 类型本身附加一个静态方法。
/// 方法签名 fn build(node: Node) -> Box<dyn Executor<T>> 恰好返回一个 trait object，
/// 因此放在 trait object 的 impl 块里语义最自然：“我（dyn Executor<T>）知道如何把自己家族的所有具体实现按需造出来”。
/// 
/// 不能放在 trait 作为静态方法
/// trait 里不能声明“构造自身”的静态方法: 静态方法没有 Self，而 Self 在 trait 定义阶段是未知大小类型（unconstrained），无法写成 Box<dyn Trait>。
/// trait 里也无法写 match 逻辑: trait 里只能声明签名，不能写实现体；默认实现又拿不到所有具体类型。
/// (仅记录，不是这里的问题)孤儿规则: 即使写成默认实现，也无法在 trait 里引用外部 crate 的 CreateTable/Insert/Scan 等实现。
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
