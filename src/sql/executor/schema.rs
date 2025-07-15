use crate::{
    error::Result,
    sql::{
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

impl Executor for CreateTable {
    fn execute(&self) -> Result<ResultSet> {
        todo!()
    }
}
