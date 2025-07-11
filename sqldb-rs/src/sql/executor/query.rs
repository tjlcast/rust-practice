use crate::sql::engine::Transaction;

use super::Executor;

pub struct Scan {
    table_name: String,
}

impl Scan {
    pub fn new(table_name: String) -> Box<Self> {
        Box::new(Self { table_name })
    }
}

impl<T: Transaction> Executor<T> for Scan {
    fn execute(&self, txn: &mut T) -> crate::error::Result<super::ResultSet> {
        todo!()
    }
}
