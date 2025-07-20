use std::collections::HashMap;

use crate::{
    error::{Error, Result},
    sql::{
        engine::Transaction,
        executor::{Executor, ResultSet},
        parser::ast::Expression,
        schema::Table,
        types::{Row, Value},
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

impl<T: Transaction> Executor<T> for Insert {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        let mut count = 0;
        let table = txn.must_get_table(self.table_name.clone())?;
        for exprs in self.values {
            // 将表达式转换成 value
            let row = exprs
                .into_iter()
                .map(|e| Value::from_expression(e))
                .collect::<Vec<_>>();
            // 如果没有指定插入的列
            let insert_row = if self.columns.is_empty() {
                pad_row(&table, &row)?
            } else {
                // 指定了插入的列，需要对 value 信息进行整理
                make_row(&table, &self.columns, &row)?
            };
            // 插入数据
            txn.create_row(self.table_name.clone(), insert_row)?;
            count += 1;
        }
        Ok(ResultSet::Insert { count })
    }
}

// 列对齐
// insert into tbl values(1, 2, 3);
// a       b        c        d
// 1       2        3    default 填充
fn pad_row(table: &Table, row: &Row) -> Result<Row> {
    let mut results = row.clone();
    for column in table.columns.iter().skip(row.len()) {
        if let Some(default) = &column.default {
            results.push(default.clone());
        } else {
            return Err(Error::Internal(format!(
                "No default value for column {}",
                column.name
            )));
        }
    }
    Ok(results)
}

// insert into tbl(d, c) values(1, 2);
//     a         b         c          d
// default    default      2          1
fn make_row(table: &Table, columns: &Vec<String>, value: &Row) -> Result<Row> {
    // 判断列数是否和value数一致
    if columns.len() != value.len() {
        return Err(Error::Internal(format!(
            "columns and values length mismatch"
        )));
    }
    let mut inputs = HashMap::new();
    for (i, col_name) in columns.iter().enumerate() {
        inputs.insert(col_name, value[i].clone());
    }
    let mut results = Vec::new();
    for col in table.columns.iter() {
        if let Some(value) = inputs.get(&col.name) {
            results.push(value.clone());
        } else if let Some(value) = &col.default {
            results.push(value.clone());
        } else {
            return Err(Error::Internal(format!(
                "No value given for column {}",
                col.name
            )));
        }
    }
    Ok(results)
}
