use crate::{
    error::RSDBResult,
    sql::{
        engine::Transaction,
        executor::{Executor, ResultSet},
        schema::Table,
    },
};

pub struct CreateTable {
    schema: Table,
}

impl CreateTable {
    pub fn new(schema: Table) -> Box<Self> {
        Box::new(Self { schema })
    }
}

impl<T: Transaction> Executor<T> for CreateTable {
    fn execute(self: Box<Self>, txn: &mut T) -> RSDBResult<ResultSet> {
        let table_name = self.schema.name.clone();
        txn.create_table(self.schema)?;
        Ok(ResultSet::CreateTable { table_name })
    }
}

pub struct DropTable {
    table_name: String,
}

impl DropTable {
    pub fn new(table_name: String) -> Box<Self> {
        Box::new(Self { table_name })
    }
}

impl<T: Transaction> Executor<T> for DropTable {
    fn execute(self: Box<Self>, txn: &mut T) -> RSDBResult<ResultSet> {
        txn.drop_table(self.table_name.clone())?;
        Ok(ResultSet::DropTable {
            table_name: self.table_name,
        })
    }
}
