pub trait Engine: Clone {
    type Transaction: Transaction;

    fn begin(&self) -> Result<Session<Self>>;

    fn session(&self) -> Result<Session<Self>> {
        Ok(Session {
            engine: self.clone(),
        })
    }
}

// 抽象的事务信息，包含了 DDL 和 DML 操作
// 底层可以接入普通的 KV 存储引擎，可以接入分布式存放引擎
pub trait Transaction {
    // 提交事务
    fn commit(&self) -> Result<()>;

    // 回滚事务
    fn rollback(&self) -> Result<()>;

    // 创建行
    fn create_row(&mut self, table: String, row )
}
