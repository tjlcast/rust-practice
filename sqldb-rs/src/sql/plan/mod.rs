use crate::error::Result;
use crate::sql::engine::Transaction;
use crate::sql::executor::Executor;
use crate::sql::{
    executor::ResultSet,
    parser::ast::{self, Expression},
    plan::planner::Planner,
    schema::Table,
};

pub mod planner;

#[derive(Debug)]
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
    },
}

// 执行计划定义，底层是不同类型执行节点
#[derive(Debug)]
pub struct Plan(pub Node);

impl Plan {
    // 使用 AST 创建一个 Plan（其中有一个node）
    pub fn build(stmt: ast::Statement) -> Self {
        Planner::new().build(stmt)
    }

    // 当这个 PLAN 执行的时候，获取其中的 Node，构建一个执行器(构建的时候进行类型自适应构建)并执行
    pub fn execute<T: Transaction>(self, txn: &mut T) -> Result<ResultSet> {
        <dyn Executor<T>>::build(self.0).execute(txn)
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
        let p1 = Plan::build(stmt1);
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
        let p2 = Plan::build(stmt2);
        println!("{:?}", p2);

        Ok(())
    }

    #[test]
    fn test_plan_insert() -> Result<()> {
        let sql1 = "
            insert into tbl1 values (1, 2, 3, 'a', true);";
        let stmt1 = Parser::new(sql1).parse()?;
        let p1 = Plan::build(stmt1);
        println!("{:?}", p1);

        let sql2 = "
            insert into tbl1 values (1, 2, 3, 'a', true);";
        let stmt2 = Parser::new(sql2).parse()?;
        let p2 = Plan::build(stmt2);
        println!("{:?}", p2);

        Ok(())
    }

    #[test]
    fn test_plan_select() -> Result<()> {
        let sql1 = "select * from tbl1;";
        let stmt1 = Parser::new(sql1).parse()?;
        let p1 = Plan::build(stmt1);
        println!("{:?}", p1);

        Ok(())
    }
}
