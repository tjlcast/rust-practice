use std::sync::Condvar;

use crate::sql::parser::ast::{Consts, Expression};

#[derive(Debug, PartialEq)]
pub enum DataType {
    Boolean,
    Integer,
    Float,
    String,
}

#[derive(Debug)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}

impl Value {
    pub fn from_expression(expr: Expression) -> Value {
        match expr {
            Expression::Consts(Consts::Null) => Self::Null,
            Expression::Consts(Consts::Boolean(b)) => Self::Boolean(b),
            Expression::Consts(Consts::Integer(i)) => Self::Integer(i),
            Expression::Consts(Consts::Float(f)) => Self::Float(f),
            Expression::Consts(Consts::String(s)) => Self::String(s),
        }
    }
}
