use std::collections::BTreeMap;

use crate::error::Result;
use crate::sql::engine::Transaction;
use crate::sql::executor::Executor;
use crate::sql::parser::ast::OrderDirection;
use crate::sql::{
    executor::ResultSet,
    parser::ast::{self, Expression},
    plan::planner::Planner,
    schema::Table,
};

pub mod planner;

#[derive(Debug, PartialEq)]
pub enum Node {
    // 创建表
    CreateTable {
        schema: Table,
    },

    // 插入数据
    Insert {
        table_name: String,
        columns: Vec<String>,
        values: Vec<Vec<Expression>>,
    },

    // 扫描节点
    Scan {
        table_name: String,
        filter: Option<(String, Expression)>,
    },

    // 更新节点
    Update {
        table_name: String,
        source: Box<Node>,
        columns: BTreeMap<String, Expression>,
    },

    // 删除节点
    Delete {
        table_name: String,
        source: Box<Node>,
    },

    // 排序节点
    Order {
        source: Box<Node>,
        order_by: Vec<(String, OrderDirection)>, // 列名，排序方式
    },

    // limit节点
    Limit {
        source: Box<Node>,
        limit: usize,
    },

    // offset 节点
    Offset {
        source: Box<Node>,
        offset: usize,
    },

    // 投影节点
    Projection {
        source: Box<Node>,
        select: Vec<(Expression, Option<String>)>, // (表达式, 可选别名)
    },

    // 嵌套循环 Join 节点
    NestedLoopJoin {
        left: Box<Node>,
        right: Box<Node>,
        predicate: Option<Expression>, // join 条件
        outer: bool,
    },

    // 聚合节点
    Aggregate {
        source: Box<Node>,
        exprs: Vec<(Expression, Option<String>)>, // (表达式, 可选别名)
        group_by: Option<Expression>,
    },
}

// 执行计划定义，底层是不同类型执行节点
#[derive(Debug, PartialEq)]
pub struct Plan(pub Node);

impl Plan {
    // 使用 AST 创建一个 Plan（其中有一个node）
    pub fn build(stmt: ast::Statement) -> Result<Self> {
        Planner::new().build(stmt)
    }

    // 当这个 PLAN 执行的时候，获取其中的 Node，构建一个执行器(构建的时候进行类型自适应构建)并执行
    pub fn execute<T: Transaction + 'static>(self, txn: &mut T) -> Result<ResultSet> {
        // let exec = <dyn Executor<T>>::build(self.0);
        let exec = Box::new(<dyn Executor<T>>::build(self.0));
        exec.execute(txn)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        error::Result,
        sql::{parser::Parser, plan::Plan},
    };

    #[test]
    fn test_plan_create_table() -> Result<()> {
        let sql1 = "
            create table tbl1 (
                a int default 100,
                b float not null,
                c varchar null,
                d bool default true
            );
        ";

        let stmt1 = Parser::new(sql1).parse()?;
        let p1 = Plan::build(stmt1)?;
        println!("{:?}", p1);

        let sql2 = "
            create                  table tbl1 (
                a int default    100,
                b float not null   ,
                c varchar     null,
                d               bool default    true
            );
        ";

        let stmt2 = Parser::new(sql2).parse()?;
        let p2 = Plan::build(stmt2)?;
        println!("{:?}", p2);

        Ok(())
    }

    #[test]
    fn test_plan_insert() -> Result<()> {
        let sql1 = "
            insert into tbl1 values (1, 2, 3, 'a', true);";
        let stmt1 = Parser::new(sql1).parse()?;
        let p1 = Plan::build(stmt1)?;
        println!("{:?}", p1);

        let sql2 = "
            insert into tbl1 values (1, 2, 3, 'a', true);";
        let stmt2 = Parser::new(sql2).parse()?;
        let p2 = Plan::build(stmt2)?;
        println!("{:?}", p2);

        Ok(())
    }

    #[test]
    fn test_plan_select() -> Result<()> {
        let sql1 = "select * from tbl1;";
        let stmt1 = Parser::new(sql1).parse()?;
        let p1 = Plan::build(stmt1)?;
        println!("{:?}", p1);

        assert_eq!(
            p1,
            Plan(crate::sql::plan::Node::Scan {
                table_name: "tbl1".to_string(),
                filter: None,
            })
        );

        Ok(())
    }
}
