use crate::{
    error::Result,
    sql::{
        executor::{mutation::Insert, query::Scan, schema::CreateTable},
        plan::Node,
        types::Row,
    },
};

mod mutation;
mod query;
mod schema;

// 执行器定义
pub trait Executor {
    fn execute(&self) -> Result<ResultSet>;
}

impl dyn Executor {
    pub fn build(node: Node) -> Box<dyn Executor> {
        match node {
            Node::CreateTable { schema } => CreateTable::new(schema),
            Node::Insert {
                table_name,
                columns,
                values,
            } => Insert::new(table_name, columns, values),
            Node::Scan { table_name } => Scan::new(table_name),
        }
    }
}

// 执行结果集
pub enum ResultSet {
    CreateTable { table_name: String },
    Insert { count: usize },
    Scan { rows: Vec<String>, row: Vec<Row> },
}
