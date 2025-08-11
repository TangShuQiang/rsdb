use crate::{
    error::RSDBResult,
    sql::{
        engine::Transaction,
        executor::{
            agg::Aggregate,
            join::NestLoopJoin,
            mutation::{Delete, Insert, Update},
            query::{Filter, Limit, Offset, Order, Projection, Scan},
            schema::CreateTable,
        },
        plan::Node,
        types::Row,
    },
};

mod agg;
mod join;
mod mutation;
mod query;
mod schema;

// 执行器定义
pub trait Executor<T: Transaction> {
    fn execute(self: Box<Self>, txn: &mut T) -> RSDBResult<ResultSet>;
}

impl<T: Transaction + 'static> dyn Executor<T> {
    pub fn build(node: Node) -> Box<dyn Executor<T>> {
        match node {
            Node::CreateTable { schema } => CreateTable::new(schema),
            Node::Insert {
                table_name,
                columns,
                values,
            } => Insert::new(table_name, columns, values),
            Node::Scan { table_name, filter } => Scan::new(table_name, filter),
            Node::Update {
                table_name,
                source,
                columns,
            } => Update::new(table_name, Self::build(*source), columns),
            Node::Delete { table_name, source } => Delete::new(table_name, Self::build(*source)),
            Node::Order { source, order_by } => Order::new(Self::build(*source), order_by),
            Node::Limit { source, limit } => Limit::new(Self::build(*source), limit),
            Node::Offset { source, offset } => Offset::new(Self::build(*source), offset),
            Node::Projection { source, exprs } => Projection::new(Self::build(*source), exprs),
            Node::NestLoopJoin {
                left,
                right,
                predicate,
                outer,
            } => NestLoopJoin::new(Self::build(*left), Self::build(*right), predicate, outer),
            Node::Aggregate {
                source,
                exprs,
                group_by,
            } => Aggregate::new(Self::build(*source), exprs, group_by),
            Node::Filter { source, predicate } => Filter::new(Self::build(*source), predicate),
        }
    }
}

// 执行结果集
#[derive(Debug, PartialEq)]
pub enum ResultSet {
    CreateTable {
        table_name: String,
    },
    Insert {
        count: usize,
    },
    Scan {
        columns: Vec<String>,
        rows: Vec<Row>,
    },
    Update {
        count: usize,
    },
    Delete {
        count: usize,
    },
}

impl ResultSet {
    pub fn to_string(&self) -> String {
        match self {
            ResultSet::CreateTable { table_name } => format!("CREATE TABLE `{}`", table_name),
            ResultSet::Insert { count } => format!("INSERT {} ROWS", count),
            ResultSet::Scan { columns, rows } => {
                let columns = columns.join(" | ");
                let rows_len = rows.len();
                let rows = rows
                    .iter()
                    .map(|row| {
                        row.iter()
                            .map(|v| v.to_string())
                            .collect::<Vec<_>>()
                            .join(" | ")
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                format!("{}\n{}\n{} ROWS", columns, rows, rows_len)
            }
            ResultSet::Update { count } => format!("UPDATE {} ROWS", count),
            ResultSet::Delete { count } => format!("DELETE {} ROWS", count),
        }
    }
}
