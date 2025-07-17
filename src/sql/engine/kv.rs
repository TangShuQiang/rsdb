use crate::{
    error::Result,
    sql::{
        engine::{Engine, Transaction},
        schema::Table,
        types::Row,
    },
    storage,
};

// KV Engin 定义
#[derive(Clone)]
pub struct KVEngine {
    pub kv: storage::Mvcc,
}

impl Engine for KVEngine {
    type Transaction = KVTransaction;

    fn begin(&self) -> Result<Self::Transaction> {
        Ok(Self::Transaction::new(self.kv.begin()?))
    }
}

// KV Transaction 定义，实际上对存储引擎中 MvccTransaction 的封装
pub struct KVTransaction {
    txn: storage::MvccTransaction,
}

impl KVTransaction {
    pub fn new(txn: storage::MvccTransaction) -> Self {
        Self { txn }
    }
}

impl Transaction for KVTransaction {
    fn commit(&self) -> Result<()> {
        todo!()
    }

    fn rollback(&self) -> Result<()> {
        todo!()
    }

    fn create_row(&self, table: String, row: Row) -> Result<()> {
        todo!()
    }

    fn scan_table(&self, table: String) -> Result<Vec<Row>> {
        todo!()
    }

    fn create_table(&self, table: Table) -> Result<()> {
        todo!()
    }

    fn get_table(&self, table_name: String) -> Result<Option<Table>> {
        todo!()
    }
}
