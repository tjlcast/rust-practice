use crate::error::{Error, Result};
use crate::sql::{
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
    pub fn build(stmt: ast::Statement) -> Self {
        Planner::new().build(stmt)
    }
}

#[cfg(test)]
mod tests {

    use crate::sql::{self, parser::Parser};

    use super::*;

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

        Ok(())
    }
}
