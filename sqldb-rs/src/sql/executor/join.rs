use crate::error::{Error, Result};
use crate::sql::parser::ast::{Expression, evaluate_expr};
use crate::sql::types::Value;
use crate::sql::{
    engine::Transaction,
    executor::{Executor, ResultSet},
};

pub struct NestedLoopJoin<T: Transaction + 'static> {
    left: Box<dyn Executor<T>>,
    right: Box<dyn Executor<T>>,
    predicate: Option<Expression>,
    outer: bool,
}

impl<T: Transaction> NestedLoopJoin<T> {
    pub fn new(
        left: Box<dyn Executor<T>>,
        right: Box<dyn Executor<T>>,
        predicate: Option<Expression>,
        outer: bool,
    ) -> Box<Self> {
        Box::new(Self {
            left,
            right,
            predicate,
            outer,
        })
    }
}

impl<T: Transaction> Executor<T> for NestedLoopJoin<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        // 先执行左边
        if let ResultSet::Scan {
            columns: lcolumns,
            rows: lrows,
        } = self.left.execute(txn)?
        {
            let mut new_columns = lcolumns.clone();
            let mut new_rows = vec![];
            // 再执行右边
            if let ResultSet::Scan {
                columns: rcolumns,
                rows: rrows,
            } = self.right.execute(txn)?
            {
                new_columns.extend(rcolumns.clone());

                for lrow in &lrows {
                    let mut matched = false;
                    for rrow in &rrows {
                        let mut new_row = lrow.clone();

                        // 如果有 Join 条件，查看是否满足 Join 条件
                        if let Some(expr) = &self.predicate {
                            match evaluate_expr(expr, &lcolumns, lrow, &rcolumns, rrow)? {
                                Value::Null => {}
                                Value::Boolean(false) => {}
                                Value::Boolean(true) => {
                                    new_row.extend(rrow.clone());
                                    new_rows.push(new_row);
                                    matched = true;
                                }
                                _ => return Err(Error::Internal("Unexpected expression".into())),
                            }
                        } else {
                            new_row.extend(rrow.clone());
                            new_rows.push(new_row);
                        }
                    }

                    if self.outer && !matched {
                        let mut new_row = lrow.clone();
                        for _ in 0..rrows[0].len() {
                            new_row.push(Value::Null);
                        }
                        new_rows.push(new_row);
                    }
                }
            }
            return Ok(ResultSet::Scan {
                columns: { new_columns },
                rows: new_rows,
            });
        }

        Err(Error::Internal("Unexpected result set".into()))
    }
}
