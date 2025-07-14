use crate::error::Result;
use crate::sql::engine::Engine;
use crate::sql::engine::Transaction;
use crate::storage::{self, engine::Engine as StorageEngine};

pub struct KVEngine<E: storage::engine::Engine> {
    pub kv: storage::mvcc::Mvcc<E>,
}

impl<E: StorageEngine> Clone for KVEngine<E> {
    fn clone(&self) -> Self {
        Self {
            kv: self.kv.clone(),
        }
    }
}

impl<E: StorageEngine> Engine for KVEngine<E> {
    type Transaction = KVTransaction<E>;

    fn begin(&self) -> Result<Self::Transaction> {
        Ok(Self::Transaction::new(self.kv.begin()?))
    }
}

pub struct KVTransaction<E: StorageEngine> {
    txn: storage::mvcc::MvccTransaction<E>,
}

impl<E: StorageEngine> KVTransaction<E> {
    pub fn new(txn: storage::mvcc::MvccTransaction<E>) -> Self {
        Self { txn }
    }
}

impl<E: StorageEngine> Transaction for KVTransaction<E> {
    fn commit(&self) -> Result<()> {
        todo!()
    }

    fn rollback(&self) -> Result<()> {
        todo!()
    }

    fn create_row(&mut self, table: String, row: crate::sql::types::Row) -> Result<()> {
        todo!()
    }

    fn scan_table(&self, table_name: String) -> Result<Vec<crate::sql::types::Row>> {
        todo!()
    }

    fn create_table(&mut self, table: crate::sql::schema::Table) -> Result<()> {
        todo!()
    }

    fn get_table(&self, table_name: String) -> Result<Option<crate::sql::schema::Table>> {
        todo!()
    }
}
