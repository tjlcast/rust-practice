use crate::{
    error::Error,
    sql::{
        parser::ast::{self, Expression, JoinType, Operation},
        plan::{Node, Plan},
        schema::{self, Table},
        types::Value,
    },
};

use crate::error::Result;

pub struct Planner;

impl Planner {
    pub fn new() -> Self {
        Self {}
    }

    pub fn build(&mut self, stmt: ast::Statement) -> Result<Plan> {
        Ok(Plan(self.build_statment(stmt)?))
    }

    fn build_statment(&self, stmt: ast::Statement) -> Result<Node> {
        Ok(match stmt {
            ast::Statement::CreateTable { name, columns } => Node::CreateTable {
                schema: Table {
                    name,
                    // for each column
                    columns: columns
                        .into_iter()
                        .map(|c| {
                            let nullable = c.nullable.unwrap_or(!c.primary_key);
                            let default = match c.default {
                                Some(expr) => Some(Value::from_expression(expr)),
                                None if nullable => Some(Value::Null),
                                None => None,
                            };

                            schema::Column {
                                name: c.name,
                                datatype: c.datatype,
                                nullable,
                                default,
                                primary_key: c.primary_key,
                            }
                        })
                        .collect(),
                },
            },
            ast::Statement::Insert {
                table_name,
                columns,
                values,
            } => Node::Insert {
                table_name,
                columns: columns.unwrap_or_default(),
                values,
            },
            ast::Statement::Select {
                select,
                from,
                where_clause,
                group_by,
                having,
                order_by,
                limit,
                offset,
            } => {
                // let mut node = Node::Scan {
                //     table_name: match from {
                //         ast::FromItem::Table { name } => name,
                //         _ => panic!("Only table is supported"),
                //     },
                //     filter: None,
                // };

                // from
                let mut node = self.build_from_item(from, &where_clause)?;

                // aggregate\group by
                let mut has_agg = false;
                if !select.is_empty() {
                    for (expr, _) in select.iter() {
                        // 如果是 Function, 说明是 agg
                        if let ast::Expression::Function(_, _) = expr {
                            has_agg = true;
                            break;
                        }
                    }
                    if group_by.is_some() {
                        has_agg = true;
                    }
                    if has_agg {
                        node = Node::Aggregate {
                            source: Box::new(node),
                            exprs: select.clone(),
                            group_by,
                        }
                    }
                }

                // having
                if let Some(expr) = having {
                    node = Node::Filter {
                        source: Box::new(node),
                        predicate: expr,
                    }
                }

                // order by
                if !order_by.is_empty() {
                    node = Node::Order {
                        source: Box::new(node),
                        order_by: order_by,
                    }
                }

                // offset
                if let Some(expr) = offset {
                    node = Node::Offset {
                        source: Box::new(node),
                        offset: match Value::from_expression(expr) {
                            Value::Integer(i) if i >= 0 => i as usize,
                            _ => 0,
                        },
                    }
                }

                // limit
                if let Some(expr) = limit {
                    node = Node::Limit {
                        source: Box::new(node),
                        limit: match Value::from_expression(expr) {
                            Value::Integer(i) if i >= 0 => i as usize,
                            _ => usize::MAX,
                        },
                    }
                }

                // projection
                if !select.is_empty() && !has_agg {
                    node = Node::Projection {
                        source: Box::new(node),
                        select: select,
                    }
                }

                node
            }
            ast::Statement::Update {
                table_name,
                columns,
                where_clause,
            } => Node::Update {
                table_name: table_name.clone(),
                columns,
                source: Box::new(Node::Scan {
                    table_name,
                    filter: where_clause,
                }),
            },
            ast::Statement::Delete {
                table_name,
                where_clause,
            } => Node::Delete {
                table_name: table_name.clone(),
                source: Box::new(Node::Scan {
                    table_name,
                    filter: where_clause,
                }),
            },
            ast::Statement::Begin | ast::Statement::Commit | ast::Statement::Rollback => {
                return Err(Error::Internal("unexpected transaction command".into()));
            }
        })
    }

    fn build_from_item(&self, item: ast::FromItem, filter: &Option<Expression>) -> Result<Node> {
        Ok(match item {
            ast::FromItem::Table { name } => Node::Scan {
                table_name: name,
                filter: filter.clone(),
            },
            ast::FromItem::Join {
                left,
                right,
                join_type,
                predicate,
            } => {
                // 如果是 Right Join的情况，则交换两个查询的位置(避免执行器重复代码)
                let (left, right) = match join_type {
                    JoinType::Right => (right, left),
                    _ => (left, right),
                };
                // 如果是 Right Join的情况，则交换Join操作的链接变量(predicate)
                let predicate = match join_type {
                    JoinType::Right => {
                        if let Some(ast::Expression::Operation(Operation::Equal(lexpr, rexpr))) =
                            predicate
                        {
                            Some(ast::Expression::Operation(Operation::Equal(rexpr, lexpr)))
                        } else {
                            predicate
                        }
                    }
                    _ => predicate,
                };

                let outer = match join_type {
                    JoinType::Cross | JoinType::Inner => false,
                    _ => true,
                };

                Node::NestedLoopJoin {
                    left: Box::new(self.build_from_item(*left, filter)?),
                    right: Box::new(self.build_from_item(*right, filter)?),
                    predicate,
                    outer,
                }
            }
        })
    }
}
