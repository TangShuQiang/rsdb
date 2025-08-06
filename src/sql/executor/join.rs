use crate::{
    error::{RSDBError, RSDBResult},
    sql::{
        engine::Transaction,
        executor::{Executor, ResultSet},
        parser::ast::{self, Expression},
        types::Value,
    },
};

pub struct NestLoopJoin<T: Transaction> {
    left: Box<dyn Executor<T>>,
    right: Box<dyn Executor<T>>,
    predicate: Option<Expression>,
    outer: bool,
}

impl<T: Transaction> NestLoopJoin<T> {
    pub fn new(
        left: Box<dyn Executor<T>>,
        right: Box<dyn Executor<T>>,
        predicate: Option<Expression>,
        outer: bool,
    ) -> Box<Self> {
        Box::new(Self {
            left,
            right,
            predicate,
            outer,
        })
    }
}

impl<T: Transaction> Executor<T> for NestLoopJoin<T> {
    fn execute(self: Box<Self>, txn: &mut T) -> RSDBResult<ResultSet> {
        // 先执行左边的
        if let ResultSet::Scan {
            columns: left_cols,
            rows: left_rows,
        } = self.left.execute(txn)?
        {
            // 再执行右边的
            if let ResultSet::Scan {
                columns: right_cols,
                rows: right_rows,
            } = self.right.execute(txn)?
            {
                let mut new_rows = Vec::new();
                let mut new_cols = left_cols.clone();
                new_cols.extend(right_cols.clone());
                for lrow in &left_rows {
                    let mut matched = false;
                    for rrow in &right_rows {
                        let mut row = lrow.clone();
                        // 如果有条件，查看是否满足 Join 条件
                        if let Some(expr) = &self.predicate {
                            match evaluate_expr(expr, &left_cols, lrow, &right_cols, rrow)? {
                                Value::Null => {}
                                Value::Boolean(false) => {}
                                Value::Boolean(true) => {
                                    row.extend(rrow.clone());
                                    new_rows.push(row);
                                    matched = true;
                                }
                                _ => {
                                    return Err(RSDBError::Internal(format!(
                                        "Join condition must evaluate to boolean, got {:?}",
                                        expr
                                    )));
                                }
                            }
                        } else {
                            row.extend(rrow.clone());
                            new_rows.push(row);
                        }
                    }
                    if self.outer && !matched {
                        let mut row = lrow.clone();
                        for _ in 0..right_cols.len() {
                            row.push(Value::Null);
                        }
                        new_rows.push(row);
                    }
                }
                return Ok(ResultSet::Scan {
                    columns: new_cols,
                    rows: new_rows,
                });
            }
        }
        Err(RSDBError::Internal(
            "Failed to execute nested loop join".to_string(),
        ))
    }
}

fn evaluate_expr(
    expr: &Expression,
    lcols: &Vec<String>,
    lrow: &Vec<Value>,
    rcols: &Vec<String>,
    rrow: &Vec<Value>,
) -> RSDBResult<Value> {
    match expr {
        Expression::Field(col_name) => {
            let pos = match lcols.iter().position(|c| c == col_name) {
                Some(pos) => pos,
                None => {
                    return Err(RSDBError::Internal(format!(
                        "Column {} not found in table",
                        col_name
                    )));
                }
            };
            Ok(lrow[pos].clone())
        }
        Expression::Operation(operation) => match operation {
            ast::Operation::Equal(lexpr, rexpr) => {
                let lval = evaluate_expr(&lexpr, lcols, lrow, rcols, rrow)?;
                let rval = evaluate_expr(&rexpr, rcols, rrow, lcols, lrow)?;
                Ok(match (lval, rval) {
                    (Value::Boolean(l), Value::Boolean(r)) => Value::Boolean(l == r),
                    (Value::Integer(l), Value::Integer(r)) => Value::Boolean(l == r),
                    (Value::Integer(l), Value::Float(r)) => Value::Boolean(l as f64 == r),
                    (Value::Float(l), Value::Integer(r)) => Value::Boolean(l == r as f64),
                    (Value::Float(l), Value::Float(r)) => Value::Boolean(l == r),
                    (Value::String(l), Value::String(r)) => Value::Boolean(l == r),
                    (Value::Null, _) => Value::Null,
                    (_, Value::Null) => Value::Null,
                    (l, r) => {
                        return Err(RSDBError::Internal(format!(
                            "Can not compare expression: {:?} and {:?}",
                            l, r
                        )));
                    }
                })
            }
        },
        _ => {
            return Err(RSDBError::Internal(format!(
                "Unsupported expression type: {:?}",
                expr
            )));
        }
    }
}
