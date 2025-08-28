use crate::{
    error::{Error, Result},
    sql::{
        engine::Transaction,
        executor::{Executor, ResultSet},
        parser::ast::Expression,
        types::Value,
    },
};

pub struct Aggregate<T: Transaction> {
    source: Box<dyn Executor<T>>,
    exprs: Vec<(Expression, Option<String>)>, // (表达式, 可选别名)
}

impl<T: Transaction> Aggregate<T> {
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

impl<T: Transaction> Executor<T> for Aggregate<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> crate::error::Result<ResultSet> {
        if let ResultSet::Scan { columns, rows } = self.source.execute(txn)? {
            let mut new_cols = Vec::new();
            let mut new_rows = Vec::new();
            for (expr, alias) in &self.exprs {
                if let Expression::Function(func_name, col_name) = expr {
                    let calculator = <dyn Calculator>::build(func_name)?;
                    let val = calculator.calc(col_name, &columns, &rows)?;

                    // min(a)               -> min
                    // min(a) as min_val    -> min_val
                    new_cols.push(if let Some(a) = alias {
                        a.clone()
                    } else {
                        func_name.clone()
                    });
                    new_rows.push(val)
                }
            }
            return Ok(ResultSet::Scan {
                columns: new_cols,
                rows: vec![new_rows],
            });
        }

        Err(Error::Internal("Unexpected result set".into()))
    }
}

// >>>>>>>>>>>>>>>>>>> Calculator trait >>>>>>>>>>>>>>>>>
pub trait Calculator {
    fn calc(&self, col_name: &String, cols: &Vec<String>, rows: &Vec<Vec<Value>>) -> Result<Value>;
}

impl dyn Calculator {
    pub fn build(func_name: &String) -> Result<Box<dyn Calculator>> {
        match func_name.to_lowercase().as_str() {
            "count" => Ok(Box::new(Count::new())),
            "min" => Ok(Box::new(Min::new())),
            "max" => Ok(Box::new(Max::new())),
            "sum" => Ok(Box::new(Sum::new())),
            "avg" => Ok(Box::new(Avg::new())),
            _ => Err(Error::Internal(format!("Unknown function: {}", func_name))),
        }
    }
}

// >>>>>>>>>>>>>>>>>>> Count >>>>>>>>>>>>>>>>>
pub struct Count;

impl Count {
    pub fn new() -> Self {
        Self {}
    }
}

impl Calculator for Count {
    fn calc(&self, col_name: &String, cols: &Vec<String>, rows: &Vec<Vec<Value>>) -> Result<Value> {
        let pos = match cols.iter().position(|c| *c == *col_name) {
            Some(pos) => pos,
            None => return Err(Error::Internal("Column not found".into())),
        };

        // a b c
        // 1 X 3.1
        // 2 NULL 6.4
        // 3 X 1.5
        let mut count = 0;
        for row in rows.iter() {
            if row[pos] != Value::Null {
                count += 1;
            }
        }

        Ok(Value::Integer(count))
    }
}

// >>>>>>>>>>>>>>>>>>> Min >>>>>>>>>>>>>>>>>
pub struct Min;

impl Min {
    pub fn new() -> Self {
        Self {}
    }
}

impl Calculator for Min {
    fn calc(&self, col_name: &String, cols: &Vec<String>, rows: &Vec<Vec<Value>>) -> Result<Value> {
        let pos = match cols.iter().position(|c| *c == *col_name) {
            Some(pos) => pos,
            None => return Err(Error::Internal("Column not found".into())),
        };

        // a b c
        // 1 X 3.1
        // 2 NULL 6.4
        // 3 X 1.5
        let mut min_value = Value::Null;
        let mut values = Vec::new();
        for row in rows.iter() {
            if row[pos] != Value::Null {
                values.push(&row[pos]);
            }
        }
        if !values.is_empty() {
            values.sort_by(|a, b| a.partial_cmp(b).unwrap());
            min_value = values[0].clone();
        }

        Ok(min_value)
    }
}

// >>>>>>>>>>>>>>>>>>> Max >>>>>>>>>>>>>>>>>
pub struct Max;

impl Max {
    pub fn new() -> Self {
        Self {}
    }
}

impl Calculator for Max {
    fn calc(&self, col_name: &String, cols: &Vec<String>, rows: &Vec<Vec<Value>>) -> Result<Value> {
        let pos = match cols.iter().position(|c| *c == *col_name) {
            Some(pos) => pos,
            None => return Err(Error::Internal("Column not found".into())),
        };

        // a b c
        // 1 X 3.1
        // 2 NULL 6.4
        // 3 X 1.5
        let mut max_value = Value::Null;
        let mut values = Vec::new();
        for row in rows.iter() {
            if row[pos] != Value::Null {
                values.push(&row[pos]);
            }
        }
        if !values.is_empty() {
            // values.sort_by(|a, b| a.partial_cmp(b).unwrap());
            // min_value = values[values.len() - 1].clone();
            values.sort_by(|a, b| b.partial_cmp(a).unwrap());
            max_value = values[0].clone();
        }

        Ok(max_value)
    }
}

// >>>>>>>>>>>>>>>>>>> Sum >>>>>>>>>>>>>>>>>
pub struct Sum;

impl Sum {
    pub fn new() -> Self {
        Self {}
    }
}

impl Calculator for Sum {
    fn calc(&self, col_name: &String, cols: &Vec<String>, rows: &Vec<Vec<Value>>) -> Result<Value> {
        let pos = match cols.iter().position(|c| *c == *col_name) {
            Some(pos) => pos,
            None => return Err(Error::Internal("Column not found".into())),
        };

        // a b c
        // 1 X 3.1
        // 2 NULL 6.4
        // 3 X 1.5
        let mut sum = None;

        for row in rows.iter() {
            match row[pos] {
                Value::Null => {}
                Value::Integer(v) => {
                    if sum == None {
                        sum = Some(0.0);
                    }
                    // 注意，这里即便是整数，这里会转换成浮点数。所以返回的合法值类型也是浮点数。
                    sum = Some(sum.unwrap() + v as f64);
                }
                Value::Float(v) => {
                    if sum == None {
                        sum = Some(0.0);
                    }
                    sum = Some(sum.unwrap() + v);
                }
                _ => {
                    return Err(Error::Internal(format!(
                        "can not calc column: {}",
                        col_name
                    )));
                }
            }
        }

        Ok(match sum {
            Some(s) => Value::Float(s),
            None => Value::Null,
        })
    }
}

// >>>>>>>>>>>>>>>>>>> Avg >>>>>>>>>>>>>>>>>
pub struct Avg;

impl Avg {
    pub fn new() -> Self {
        Self {}
    }
}

impl Calculator for Avg {
    fn calc(&self, col_name: &String, cols: &Vec<String>, rows: &Vec<Vec<Value>>) -> Result<Value> {
        let sum_value = Sum::new().calc(col_name, cols, rows)?;
        let count_value = Count::new().calc(col_name, cols, rows)?;

        Ok(match (sum_value, count_value) {
            (Value::Float(sum_value), Value::Integer(count_value)) => {
                Value::Float(sum_value / count_value as f64)
            }
            _ => Value::Null,
        })
    }
}
