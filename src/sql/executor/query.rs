use std::{cmp::Ordering, collections::HashMap};

use crate::{
    error::{RSDBError, RSDBResult},
    sql::{
        engine::Transaction,
        executor::{Executor, ResultSet},
        parser::ast::{Expression, OrderDirection, evaluate_expr},
        types::Value,
    },
};

pub struct Scan {
    table_name: String,
    filter: Option<Expression>,
}

impl Scan {
    pub fn new(table_name: String, filter: Option<Expression>) -> Box<Self> {
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

pub struct Limit<T: Transaction> {
    source: Box<dyn Executor<T>>,
    limit: usize,
}

impl<T: Transaction> Limit<T> {
    pub fn new(source: Box<dyn Executor<T>>, limit: usize) -> Box<Self> {
        Box::new(Self { source, limit })
    }
}

impl<T: Transaction> Executor<T> for Limit<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> RSDBResult<ResultSet> {
        match self.source.execute(txn)? {
            ResultSet::Scan { columns, rows } => Ok(ResultSet::Scan {
                columns,
                rows: rows.into_iter().take(self.limit).collect(),
            }),
            _ => {
                return Err(RSDBError::Internal(
                    "Limit source must be a Scan".to_string(),
                ));
            }
        }
    }
}

pub struct Offset<T: Transaction> {
    source: Box<dyn Executor<T>>,
    offset: usize,
}

impl<T: Transaction> Offset<T> {
    pub fn new(source: Box<dyn Executor<T>>, offset: usize) -> Box<Self> {
        Box::new(Self { source, offset })
    }
}

impl<T: Transaction> Executor<T> for Offset<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> RSDBResult<ResultSet> {
        match self.source.execute(txn)? {
            ResultSet::Scan { columns, rows } => Ok(ResultSet::Scan {
                columns,
                rows: rows.into_iter().skip(self.offset).collect(),
            }),
            _ => {
                return Err(RSDBError::Internal(
                    "Offset source must be a Scan".to_string(),
                ));
            }
        }
    }
}

pub struct Projection<T: Transaction> {
    source: Box<dyn Executor<T>>,
    exprs: Vec<(Expression, Option<String>)>,
}

impl<T: Transaction> Projection<T> {
    pub fn new(
        source: Box<dyn Executor<T>>,
        exprs: Vec<(Expression, Option<String>)>,
    ) -> Box<Self> {
        Box::new(Self { source, exprs })
    }
}

impl<T: Transaction> Executor<T> for Projection<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> RSDBResult<ResultSet> {
        match self.source.execute(txn)? {
            ResultSet::Scan { columns, rows } => {
                // 找到需要输出哪些列
                let mut selected = Vec::new();
                let mut new_columns = Vec::new();
                for (expr, alias) in self.exprs {
                    if let Expression::Field(col_name) = expr {
                        let pos = match columns.iter().position(|c| *c == col_name) {
                            Some(pos) => pos,
                            None => {
                                return Err(RSDBError::Internal(format!(
                                    "column {} not found in table",
                                    col_name
                                )));
                            }
                        };
                        selected.push(pos);
                        if let Some(a) = alias {
                            new_columns.push(a);
                        } else {
                            new_columns.push(col_name);
                        }
                    }
                }
                let mut new_rows = Vec::new();
                for row in rows.into_iter() {
                    let mut new_row = Vec::new();
                    for i in selected.iter() {
                        new_row.push(row[*i].clone());
                    }
                    new_rows.push(new_row);
                }
                Ok(ResultSet::Scan {
                    columns: new_columns,
                    rows: new_rows,
                })
            }
            _ => {
                return Err(RSDBError::Internal(
                    "Project source must be a Scan".to_string(),
                ));
            }
        }
    }
}

pub struct Filter<T: Transaction> {
    source: Box<dyn Executor<T>>,
    predicate: Expression,
}

impl<T: Transaction> Filter<T> {
    pub fn new(source: Box<dyn Executor<T>>, predicate: Expression) -> Box<Self> {
        Box::new(Self { source, predicate })
    }
}

impl<T: Transaction> Executor<T> for Filter<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> RSDBResult<ResultSet> {
        match self.source.execute(txn)? {
            ResultSet::Scan { columns, rows } => {
                let mut new_rows = Vec::new();
                for row in rows {
                    match evaluate_expr(&self.predicate, &columns, &row, &columns, &row)? {
                        Value::Null => {}
                        Value::Boolean(false) => {}
                        Value::Boolean(true) => {
                            new_rows.push(row);
                        }
                        _ => {
                            return Err(RSDBError::Internal(
                                "
                                Predicate must evaluate to a boolean value"
                                    .to_string(),
                            ));
                        }
                    }
                }
                Ok(ResultSet::Scan {
                    columns,
                    rows: new_rows,
                })
            }
            _ => {
                return Err(RSDBError::Internal(
                    "Filter source must be a Scan".to_string(),
                ));
            }
        }
    }
}

pub struct IndexScan {
    table_name: String,
    field: String,
    value: Value,
}

impl IndexScan {
    pub fn new(table_name: String, field: String, value: Value) -> Box<Self> {
        Box::new(Self {
            table_name,
            field,
            value,
        })
    }
}

impl<T: Transaction> Executor<T> for IndexScan {
    fn execute(self: Box<Self>, txn: &mut T) -> RSDBResult<ResultSet> {
        let table = txn.must_get_table(self.table_name.clone())?;
        let index = txn.load_index(&self.table_name, &self.field, &self.value)?;
        let mut pks = index.iter().collect::<Vec<_>>();
        pks.sort_by(|v1, v2| match v1.partial_cmp(v2) {
            Some(ord) => ord,
            None => Ordering::Equal,
        });
        let mut rows = Vec::new();
        for pk in pks {
            if let Some(row) = txn.read_by_pk(&self.table_name, pk)? {
                rows.push(row);
            }
        }
        Ok(ResultSet::Scan {
            columns: table.columns.into_iter().map(|c| c.name).collect(),
            rows,
        })
    }
}
