use crate::sql::{
    parser::ast,
    plan::{Node, Plan},
};

pub struct Planner;

impl Planner {
    pub fn new() -> Self {
        Self {}
    }

    pub fn build(&mut self, stmt: ast::Statement) -> Plan {
        Plan(self.build_statment(stmt))
    }

    fn build_statment(&self, stmt: ast::Statement) -> Node {
        match stmt {
            ast::Statement::CreateTable { name: table_name, columns } => {
                schema: Table {
                    name: table_name,
                    // for each column
                    columns: columns.into_iter().map(|c| {
                        let nullable = c.nullable.unwrap_or(true);
                        match c.default {
                            Some(expr) => todo!(),
                            None if nullable => Some(Value::Null),
                            None => None,
                        }
                    })
                }
            },
            ast::Statement::Insert {
                table_name,
                columns,
                values,
            } => todo!(),
            ast::Statement::Select {
                table_name,
            } => todo!(),
        }
    }
}
