use schema::CreateTable;

use crate::{
    error::Result,
    sql::{
        engine::Transaction,
        executor::{
            join::NestedLoopJoin,
            mutation::{Delete, Insert, Update},
            query::{Limit, Offset, Order, Projection, Scan},
        },
    },
};

use super::{plan::Node, types::Row};

mod agg;
mod join;
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
    CreateTable {
        table_name: String,
    },

    Insert {
        count: usize,
    },

    Scan {
        columns: Vec<String>,
        rows: Vec<Row>,
    },
    Update {
        count: usize,
    },
    Delete {
        count: usize,
    },
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
///
/// 没有Update的时候Rust能自动推导
///     具体类型：每个分支都返回具体的结构体类型（CreateTable、Insert、Scan）
///     单态化：Rust知道这些具体类型的完整信息，包括它们的生命周期
///     自动满足约束：具体类型隐式满足'static约束（因为它们不包含引用）
///     统一到trait object：编译器可以安全地将具体类型装箱为Box<dyn Executor<T>>
///     Self::build(*source)返回的是Box<dyn Executor<T>>（trait object），而不是具体类型。
///     编译器看到：1\需要返回Box<dyn Executor<T> + 'static>;2\但T的类型参数没有生命周期约束;3\T中可能包含非'static的引用
///
/// 编译器的心智模型
/// 没有Update时：
///     具体类型 → 自动满足 'static → 可以装箱为 dyn Executor<T>
/// 有Update时：
///     递归调用 → 返回 dyn Executor<T> → 需要 T: 'static。但 T 没有约束 → 编译错误！
impl<T: Transaction + 'static> dyn Executor<T> {
    // 把sql计划转化为sql执行器
    pub fn build(node: Node) -> Box<dyn Executor<T>> {
        match node {
            Node::CreateTable { schema } => CreateTable::new(schema),
            Node::Insert {
                table_name,
                columns,
                values,
            } => Insert::new(table_name, columns, values),
            Node::Scan { table_name, filter } => Scan::new(table_name, filter),
            Node::Order { source, order_by } => Order::new(Self::build(*source), order_by),
            Node::Update {
                table_name,
                source,
                columns,
            } => Update::new(
                table_name,
                // 注意这里有一个递归，涉及到trait object的生命周期擦除
                Self::build(*source),
                columns,
            ),
            Node::Delete { table_name, source } => Delete::new(
                table_name,
                // 注意这里有一个递归，涉及到trait object的生命周期擦除
                Self::build(*source),
            ),
            Node::Limit { source, limit } => Limit::new(Self::build(*source), limit),
            Node::Offset { source, offset } => Offset::new(Self::build(*source), offset),
            Node::Projection { source, select } => Projection::new(Self::build(*source), select),
            Node::NestedLoopJoin {
                left,
                right,
                predicate,
                outer,
            } => NestedLoopJoin::new(Self::build(*left), Self::build(*right), predicate, outer),
            Node::Aggregate {
                source,
                exprs,
                group_by,
            } => agg::Aggregate::new(Self::build(*source), exprs, group_by),
        }
    }
}
