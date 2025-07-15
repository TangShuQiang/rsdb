use crate::{
    error::Result,
    sql::{
        executor::{Executor, ResultSet},
        parser::ast::Expression,
    },
};

pub struct Insert {
    table_name: String,
    columns: Vec<String>,
    values: Vec<Vec<Expression>>,
}

impl Insert {
    pub fn new(
        table_name: String,
        columns: Vec<String>,
        values: Vec<Vec<Expression>>,
    ) -> Box<Self> {
        Box::new(Self {
            table_name,
            columns,
            values,
        })
    }
}

impl Executor for Insert {
    fn execute(&self) -> Result<ResultSet> {
        todo!();
    }
}
