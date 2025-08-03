use std::{cmp::Ordering, collections::HashMap};

use crate::{
    error::{RSDBError, RSDBResult},
    sql::{
        engine::Transaction,
        executor::{Executor, ResultSet},
        parser::ast::{Expression, OrderDirection},
    },
};

pub struct Scan {
    table_name: String,
    filter: Option<(String, Expression)>,
}

impl Scan {
    pub fn new(table_name: String, filter: Option<(String, Expression)>) -> Box<Self> {
        Box::new(Self { table_name, filter })
    }
}

impl<T: Transaction> Executor<T> for Scan {
    fn execute(self: Box<Self>, txn: &mut T) -> RSDBResult<ResultSet> {
        let table = txn.must_get_table(self.table_name.clone())?;
        let rows = txn.scan_table(&table, self.filter)?;
        Ok(ResultSet::Scan {
            columns: table.columns.into_iter().map(|c| c.name).collect(),
            rows,
        })
    }
}

pub struct Order<T: Transaction> {
    source: Box<dyn Executor<T>>,
    order_by: Vec<(String, OrderDirection)>,
}

impl<T: Transaction> Order<T> {
    pub fn new(source: Box<dyn Executor<T>>, order_by: Vec<(String, OrderDirection)>) -> Box<Self> {
        Box::new(Self { source, order_by })
    }
}

impl<T: Transaction> Executor<T> for Order<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> RSDBResult<ResultSet> {
        match self.source.execute(txn)? {
            ResultSet::Scan { columns, mut rows } => {
                // 找到 order by 的列对应表中的列的位置
                let mut order_col_index = HashMap::new();
                for (i, (col_name, _)) in self.order_by.iter().enumerate() {
                    match columns.iter().position(|c| *c == *col_name) {
                        Some(pos) => order_col_index.insert(i, pos),
                        None => {
                            return Err(RSDBError::Internal(format!(
                                "order by column {} not found in table",
                                col_name
                            )));
                        }
                    };
                }
                rows.sort_by(|col1, col2| {
                    for (i, (_, direction)) in self.order_by.iter().enumerate() {
                        let col_index = order_col_index.get(&i).unwrap();
                        let x = &col1[*col_index];
                        let y = &col2[*col_index];
                        match x.partial_cmp(y) {
                            Some(std::cmp::Ordering::Equal) => {}
                            Some(o) => {
                                return if *direction == OrderDirection::Asc {
                                    o
                                } else {
                                    o.reverse()
                                };
                            }
                            None => {}
                        }
                    }
                    Ordering::Equal
                });
                Ok(ResultSet::Scan { columns, rows })
            }
            _ => {
                return Err(RSDBError::Internal(
                    "Order source must be a Scan".to_string(),
                ));
            }
        }
    }
}
