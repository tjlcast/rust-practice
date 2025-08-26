use crate::error::{Error, Result};
use crate::sql::{
    engine::Transaction,
    executor::{Executor, ResultSet},
};

pub struct NestedLoopJoin<T: Transaction + 'static> {
    left: Box<dyn Executor<T>>,
    right: Box<dyn Executor<T>>,
}

impl<T: Transaction> NestedLoopJoin<T> {
    pub fn new(left: Box<dyn Executor<T>>, right: Box<dyn Executor<T>>) -> Box<Self> {
        Box::new(Self { left, right })
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
            let mut new_columns = lcolumns;
            let mut new_rows = vec![];
            // 再执行右边
            if let ResultSet::Scan {
                columns: rcolumns,
                rows: rrows,
            } = self.right.execute(txn)?
            {
                new_columns.extend(rcolumns);

                for lrow in &lrows {
                    for rrow in &rrows {
                        let mut row = lrow.clone();
                        row.extend(rrow.clone());
                        new_rows.push(row);
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
