use std::collections::BTreeMap;

use crate::{
    error::{Error, Result},
    sql::types::{DataType, Value},
};

#[derive(Debug, PartialEq)]
pub enum Statement {
    CreateTable {
        name: String,
        columns: Vec<Column>,
    },
    Insert {
        table_name: String,
        columns: Option<Vec<String>>,
        values: Vec<Vec<Expression>>,
    },
    Select {
        select: Vec<(Expression, Option<String>)>, // (表达式, 可选别名)
        from: FromItem,
        where_clause: Option<Expression>,
        group_by: Option<Expression>,
        having: Option<Expression>,
        order_by: Vec<(String, OrderDirection)>,
        limit: Option<Expression>,
        offset: Option<Expression>,
    },
    Update {
        table_name: String,
        columns: BTreeMap<String, Expression>,
        where_clause: Option<Expression>,
    },
    Delete {
        table_name: String,
        where_clause: Option<Expression>,
    },
}

#[derive(Debug, PartialEq)]
pub enum OrderDirection {
    Asc,
    Desc,
}

#[derive(Debug, PartialEq)]
pub enum FromItem {
    Table {
        name: String,
    },
    Join {
        left: Box<FromItem>,
        right: Box<FromItem>,
        join_type: JoinType,
        predicate: Option<Expression>,
    },
}

#[derive(Debug, PartialEq)]
pub enum JoinType {
    Cross,
    Inner,
    Left,
    Right,
}

#[derive(Debug, PartialEq)]
pub struct Column {
    pub name: String,
    pub datatype: DataType,
    pub nullable: Option<bool>,
    pub default: Option<Expression>,
    pub primary_key: bool,
}

// 表达式定义，目前只有常量和列名
#[derive(Debug, PartialEq, Clone)]
pub enum Expression {
    Field(String),
    Consts(Consts),
    Operation(Operation),     // 在 join 的情况下
    Function(String, String), // 在 agg 的情况下
}

impl From<Consts> for Expression {
    fn from(value: Consts) -> Self {
        Self::Consts(value)
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum Consts {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}

#[derive(Debug, PartialEq, Clone)]
pub enum Operation {
    Equal(Box<Expression>, Box<Expression>),
    GreaterThan(Box<Expression>, Box<Expression>),
    LessThan(Box<Expression>, Box<Expression>),
}

pub fn evaluate_expr(
    expr: &Expression,
    lcols: &Vec<String>,
    lrow: &Vec<Value>,
    rcols: &Vec<String>,
    rrow: &Vec<Value>,
) -> Result<Value> {
    match expr {
        Expression::Field(col_name) => {
            let lcol_pos = match lcols.iter().position(|c| *c == *col_name) {
                Some(pos) => pos,
                None => {
                    return Err(Error::Internal(format!(
                        "Column {} not found in table",
                        col_name
                    )));
                }
            };
            Ok(lrow[lcol_pos].clone())
        }
        Expression::Consts(consts) => Ok(match consts {
            Consts::Null => Value::Null,
            Consts::Boolean(b) => Value::Boolean(*b),
            Consts::Integer(i) => Value::Integer(*i),
            Consts::Float(f) => Value::Float(*f),
            Consts::String(s) => Value::String(s.clone()),
        }),
        Expression::Operation(operation) => match operation {
            Operation::Equal(lexpr, rexpr) => {
                let lv = evaluate_expr(lexpr, lcols, lrow, rcols, rrow)?;
                let rv = evaluate_expr(rexpr, rcols, rrow, lcols, lrow)?;
                Ok(match (lv, rv) {
                    // (Value::Null, _) | (_, Value::Null) => Ok(Value::Bool(false)),
                    (Value::Boolean(l), Value::Boolean(r)) => Value::Boolean(l == r),
                    (Value::Integer(l), Value::Integer(r)) => Value::Boolean(l == r),
                    (Value::Integer(l), Value::Float(r)) => Value::Boolean(l as f64 == r),
                    (Value::Float(l), Value::Integer(r)) => Value::Boolean(l == r as f64),
                    (Value::Float(l), Value::Float(r)) => Value::Boolean(l == r),
                    (Value::String(l), Value::String(r)) => Value::Boolean(l == r),
                    (_, Value::Null) => Value::Null,
                    (Value::Null, _) => Value::Null,
                    (l, r) => {
                        return Err(Error::Internal(format!(
                            "can not compare expression {} and {}",
                            l, r
                        )));
                    }
                })
            }
            Operation::GreaterThan(lexpr, rexpr) => {
                let lv = evaluate_expr(lexpr, lcols, lrow, rcols, rrow)?;
                let rv = evaluate_expr(rexpr, rcols, rrow, lcols, lrow)?;
                Ok(match (lv, rv) {
                    // (Value::Null, _) | (_, Value::Null) => Ok(Value::Bool(false)),
                    (Value::Boolean(l), Value::Boolean(r)) => Value::Boolean(l > r),
                    (Value::Integer(l), Value::Integer(r)) => Value::Boolean(l > r),
                    (Value::Integer(l), Value::Float(r)) => Value::Boolean(l as f64 > r),
                    (Value::Float(l), Value::Integer(r)) => Value::Boolean(l > r as f64),
                    (Value::Float(l), Value::Float(r)) => Value::Boolean(l > r),
                    (Value::String(l), Value::String(r)) => Value::Boolean(l > r),
                    (_, Value::Null) => Value::Null,
                    (Value::Null, _) => Value::Null,
                    (l, r) => {
                        return Err(Error::Internal(format!(
                            "can not compare expression {} and {}",
                            l, r
                        )));
                    }
                })
            }
            Operation::LessThan(lexpr, rexpr) => {
                let lv = evaluate_expr(lexpr, lcols, lrow, rcols, rrow)?;
                let rv = evaluate_expr(rexpr, rcols, rrow, lcols, lrow)?;
                Ok(match (lv, rv) {
                    // (Value::Null, _) | (_, Value::Null) => Ok(Value::Bool(false)),
                    (Value::Boolean(l), Value::Boolean(r)) => Value::Boolean(l < r),
                    (Value::Integer(l), Value::Integer(r)) => Value::Boolean(l < r),
                    (Value::Integer(l), Value::Float(r)) => Value::Boolean((l as f64) < r),
                    (Value::Float(l), Value::Integer(r)) => Value::Boolean(l < r as f64),
                    (Value::Float(l), Value::Float(r)) => Value::Boolean(l < r),
                    (Value::String(l), Value::String(r)) => Value::Boolean(l < r),
                    (_, Value::Null) => Value::Null,
                    (Value::Null, _) => Value::Null,
                    (l, r) => {
                        return Err(Error::Internal(format!(
                            "can not compare expression {} and {}",
                            l, r
                        )));
                    }
                })
            }
        },
        _ => Err(Error::Internal(
            "Unsupported expression in join predicate".into(),
        )),
    }
}
