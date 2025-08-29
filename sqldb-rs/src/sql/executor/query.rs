use std::{cmp::Ordering, collections::HashMap};

use crate::{
    error::Error,
    sql::{
        engine::Transaction,
        executor::ResultSet,
        parser::ast::{Expression, OrderDirection},
    },
};

use super::Executor;

pub struct Scan {
    table_name: String,
    filter: Option<Expression>,
}

impl Scan {
    pub fn new(table_name: String, filter: Option<Expression>) -> Box<Self> {
        Box::new(Self { table_name, filter })
    }
}

impl<T: Transaction> Executor<T> for Scan {
    fn execute(self: Box<Self>, txn: &mut T) -> crate::error::Result<super::ResultSet> {
        let table = txn.must_get_table(self.table_name.clone())?;
        let rows = txn.scan_table(self.table_name.clone(), self.filter)?;
        Ok(ResultSet::Scan {
            columns: table.columns.into_iter().map(|c| c.name.clone()).collect(),
            rows,
        })
    }
}

pub struct Order<T: Transaction> {
    source: Box<dyn Executor<T>>,
    order_by: Vec<(String, OrderDirection)>,
}

impl<T: Transaction> Order<T> {
    pub fn new(source: Box<dyn Executor<T>>, order_by: Vec<(String, OrderDirection)>) -> Box<Self> {
        Box::new(Self { source, order_by })
    }
}

impl<T: Transaction> Executor<T> for Order<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> crate::error::Result<ResultSet> {
        match self.source.execute(txn)? {
            ResultSet::Scan { columns, mut rows } => {
                // 找到 order_by 的列对应表中的位置
                let mut order_col_index = HashMap::new();
                for (i, (col_name, _)) in self.order_by.iter().enumerate() {
                    match columns.iter().position(|c| *c == *col_name) {
                        Some(pos) => order_col_index.insert(i, pos),
                        None => {
                            return Err(Error::Internal(format!(
                                "order by column {} is not in table",
                                col_name
                            )));
                        }
                    };
                }

                rows.sort_by(|a, b| {
                    for (i, (_, direction)) in self.order_by.iter().enumerate() {
                        let col_index = order_col_index.get(&i).unwrap();
                        let x = &a[*col_index];
                        let y = &b[*col_index];
                        match x.partial_cmp(y) {
                            Some(Ordering::Equal) => {}
                            Some(order) => {
                                return if *direction == OrderDirection::Asc {
                                    order
                                } else {
                                    order.reverse()
                                };
                            }
                            None => {}
                        }
                    }
                    Ordering::Equal
                });

                Ok(ResultSet::Scan { columns, rows })
            }
            _ => return Err(Error::Internal("Unexpected result set".into())),
        }
    }
}

pub struct Limit<T: Transaction> {
    source: Box<dyn Executor<T>>,
    limit: usize,
}

impl<T: Transaction> Limit<T> {
    pub fn new(source: Box<dyn Executor<T>>, limit: usize) -> Box<Self> {
        Box::new(Self { source, limit })
    }
}

impl<T: Transaction> Executor<T> for Limit<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> crate::error::Result<ResultSet> {
        match self.source.execute(txn)? {
            ResultSet::Scan { columns, rows } => {
                // if rows.len() > self.limit {
                //     rows.truncate(self.limit);
                // }
                // Ok(ResultSet::Scan { columns, rows })
                Ok(ResultSet::Scan {
                    columns: columns,
                    rows: rows.into_iter().take(self.limit).collect(),
                })
            }
            _ => return Err(Error::Internal("Unexpected result set".into())),
        }
    }
}

pub struct Offset<T: Transaction> {
    source: Box<dyn Executor<T>>,
    offset: usize,
}

impl<T: Transaction> Offset<T> {
    pub fn new(source: Box<dyn Executor<T>>, offset: usize) -> Box<Self> {
        Box::new(Self { source, offset })
    }
}

impl<T: Transaction> Executor<T> for Offset<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> crate::error::Result<ResultSet> {
        match self.source.execute(txn)? {
            ResultSet::Scan { columns, rows } => {
                // if rows.len() > self.offset {
                //     rows.drain(0..self.offset);
                // }
                // Ok(ResultSet::Scan { columns, rows })
                Ok(ResultSet::Scan {
                    columns: columns,
                    rows: rows.into_iter().skip(self.offset).collect(),
                })
            }
            _ => return Err(Error::Internal("Unexpected result set".into())),
        }
    }
}

pub struct Projection<T: Transaction> {
    source: Box<dyn Executor<T>>,
    exprs: Vec<(Expression, Option<String>)>, // (表达式, 可选别名)
}

impl<T: Transaction> Projection<T> {
    pub fn new(
        source: Box<dyn Executor<T>>,
        select: Vec<(Expression, Option<String>)>,
    ) -> Box<Self> {
        Box::new(Self {
            source,
            exprs: select,
        })
    }
}

impl<T: Transaction> Executor<T> for Projection<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> crate::error::Result<ResultSet> {
        match self.source.execute(txn)? {
            ResultSet::Scan { columns, rows } => {
                // 找到需要输出哪些列
                let mut selected = Vec::new();
                let mut new_columns = Vec::new();
                for (expr, alias) in self.exprs {
                    if let Expression::Field(col_name) = expr {
                        let pos = match columns.iter().position(|c| *c == col_name) {
                            Some(pos) => pos,
                            None => {
                                return Err(Error::Internal(format!(
                                    "projection column {} is not in table",
                                    col_name
                                )));
                            }
                        };
                        selected.push(pos);
                        new_columns.push(if alias.is_some() {
                            alias.unwrap()
                        } else {
                            col_name
                        });
                    }
                }

                let mut new_rows = Vec::new();
                for row in rows.into_iter() {
                    let mut new_row = Vec::new();
                    for i in selected.iter() {
                        new_row.push(row[*i].clone());
                    }
                    new_rows.push(new_row);
                }

                Ok(ResultSet::Scan {
                    columns: new_columns,
                    rows: new_rows,
                })
            }
            _ => return Err(Error::Internal(format!("Unexpected result set"))),
        }
    }
}
