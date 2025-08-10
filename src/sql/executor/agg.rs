use crate::{
    error::{RSDBError, RSDBResult},
    sql::{
        engine::Transaction,
        executor::{Executor, ResultSet},
        parser::ast::{self, Expression},
        types::Value,
    },
};

pub struct Aggregate<T: Transaction> {
    source: Box<dyn Executor<T>>,
    exprs: Vec<(Expression, Option<String>)>,
}

impl<T: Transaction> Aggregate<T> {
    pub fn new(
        source: Box<dyn Executor<T>>,
        exprs: Vec<(Expression, Option<String>)>,
    ) -> Box<Self> {
        Box::new(Self { source, exprs })
    }
}

impl<T: Transaction> Executor<T> for Aggregate<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> RSDBResult<ResultSet> {
        if let ResultSet::Scan { columns, rows } = self.source.execute(txn)? {
            let mut new_cols = Vec::new();
            let mut new_rows = Vec::new();
            for (expr, alias) in self.exprs {
                if let ast::Expression::Function(func_name, col_name) = expr {
                    let calculator = <dyn Calculator>::build(&func_name)?;
                    let val = calculator.calc(&col_name, &columns, &rows)?;
                    new_cols.push(alias.unwrap_or(format!(
                        "{}({})",
                        func_name.to_uppercase(),
                        col_name
                    )));
                    new_rows.push(val);
                }
            }
            return Ok(ResultSet::Scan {
                columns: new_cols,
                rows: vec![new_rows],
            });
        }
        Err(RSDBError::Internal(
            "Aggregate source must be a Scan".to_string(),
        ))
    }
}

// 通用 Agg 计算定义
pub trait Calculator {
    fn calc(
        &self,
        col_name: &String,
        cols: &Vec<String>,
        rows: &Vec<Vec<Value>>,
    ) -> RSDBResult<Value>;
}

impl dyn Calculator {
    pub fn build(func_name: &String) -> RSDBResult<Box<dyn Calculator>> {
        Ok(match func_name.to_uppercase().as_ref() {
            "COUNT" => Count::new(),
            "MIN" => Min::new(),
            "MAX" => Max::new(),
            "SUM" => Sum::new(),
            "AVG" => Avg::new(),
            _ => {
                return Err(RSDBError::Internal(format!(
                    "unknown aggregate function {}",
                    func_name
                )));
            }
        })
    }
}

pub struct Count;

impl Count {
    fn new() -> Box<Self> {
        Box::new(Self {})
    }
}

impl Calculator for Count {
    fn calc(
        &self,
        col_name: &String,
        cols: &Vec<String>,
        rows: &Vec<Vec<Value>>,
    ) -> RSDBResult<Value> {
        let pos = match cols.iter().position(|c| c == col_name) {
            Some(pos) => pos,
            None => {
                return Err(RSDBError::Internal(format!(
                    "column {} not found",
                    col_name
                )));
            }
        };
        let mut count = 0;
        for row in rows {
            if row[pos] != Value::Null {
                count += 1;
            }
        }
        Ok(Value::Integer(count))
    }
}

pub struct Min;

impl Min {
    fn new() -> Box<Self> {
        Box::new(Self {})
    }
}

impl Calculator for Min {
    fn calc(
        &self,
        col_name: &String,
        cols: &Vec<String>,
        rows: &Vec<Vec<Value>>,
    ) -> RSDBResult<Value> {
        let pos = match cols.iter().position(|c| c == col_name) {
            Some(pos) => pos,
            None => {
                return Err(RSDBError::Internal(format!(
                    "column {} not found",
                    col_name
                )));
            }
        };
        let mut min_val = Value::Null;
        let mut values = Vec::new();
        for row in rows.iter() {
            if row[pos] != Value::Null {
                values.push(&row[pos]);
            }
        }
        if !values.is_empty() {
            values.sort_by(|a, b| a.partial_cmp(b).unwrap());
            min_val = values[0].clone();
        }
        Ok(min_val)
    }
}

pub struct Max;

impl Max {
    fn new() -> Box<Self> {
        Box::new(Self {})
    }
}

impl Calculator for Max {
    fn calc(
        &self,
        col_name: &String,
        cols: &Vec<String>,
        rows: &Vec<Vec<Value>>,
    ) -> RSDBResult<Value> {
        let pos = match cols.iter().position(|c| c == col_name) {
            Some(pos) => pos,
            None => {
                return Err(RSDBError::Internal(format!(
                    "column {} not found",
                    col_name
                )));
            }
        };
        let mut max_val = Value::Null;
        let mut values = Vec::new();
        for row in rows.iter() {
            if row[pos] != Value::Null {
                values.push(&row[pos]);
            }
        }
        if !values.is_empty() {
            values.sort_by(|a, b| a.partial_cmp(b).unwrap());
            max_val = values[values.len() - 1].clone();
        }
        Ok(max_val)
    }
}

pub struct Sum;

impl Sum {
    fn new() -> Box<Self> {
        Box::new(Self {})
    }
}

impl Calculator for Sum {
    fn calc(
        &self,
        col_name: &String,
        cols: &Vec<String>,
        rows: &Vec<Vec<Value>>,
    ) -> RSDBResult<Value> {
        let pos = match cols.iter().position(|c| c == col_name) {
            Some(pos) => pos,
            None => {
                return Err(RSDBError::Internal(format!(
                    "column {} not found",
                    col_name
                )));
            }
        };
        let mut sum = None;
        for row in rows.iter() {
            match row[pos] {
                Value::Null => continue,
                Value::Integer(i) => {
                    if sum == None {
                        sum = Some(0.0);
                    }
                    sum = Some(sum.unwrap() + i as f64);
                }
                Value::Float(f) => {
                    if sum == None {
                        sum = Some(0.0);
                    }
                    sum = Some(sum.unwrap() + f);
                }
                _ => {
                    return Err(RSDBError::Internal(format!(
                        "column {} is not numeric",
                        col_name
                    )));
                }
            }
        }
        if let Some(s) = sum {
            Ok(Value::Float(s))
        } else {
            Ok(Value::Null)
        }
    }
}

pub struct Avg;

impl Avg {
    fn new() -> Box<Self> {
        Box::new(Self {})
    }
}

impl Calculator for Avg {
    fn calc(
        &self,
        col_name: &String,
        cols: &Vec<String>,
        rows: &Vec<Vec<Value>>,
    ) -> RSDBResult<Value> {
        let sum = Sum::new().calc(col_name, cols, rows)?;
        let count = Count::new().calc(col_name, cols, rows)?;
        Ok(match (sum, count) {
            (Value::Float(s), Value::Integer(c)) => Value::Float(s / c as f64),
            _ => Value::Null,
        })
    }
}
