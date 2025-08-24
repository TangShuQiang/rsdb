use std::collections::HashMap;

use crate::{
    error::{RSDBError, RSDBResult},
    sql::{
        engine::Transaction,
        executor::{Executor, ResultSet},
        parser::ast::{self, Expression, evaluate_expr},
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

pub struct HashJoin<T: Transaction> {
    left: Box<dyn Executor<T>>,
    right: Box<dyn Executor<T>>,
    predicate: Option<Expression>,
    outer: bool,
}

impl<T: Transaction> HashJoin<T> {
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

impl<T: Transaction> Executor<T> for HashJoin<T> {
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
                // 解析 HashJoin 条件
                let (left_field, right_field) = match parse_join_filter(self.predicate) {
                    Some(filter) => filter,
                    None => {
                        return Err(RSDBError::Internal(
                            "failed to parse join predicate".to_string(),
                        ));
                    }
                };
                // 获取 join 列在表中的位置
                let lpos = match left_cols.iter().position(|c| *c == left_field) {
                    Some(pos) => pos,
                    None => {
                        return Err(RSDBError::Internal(format!(
                            "Join field '{}' not found in table",
                            left_field
                        )));
                    }
                };
                let rpos = match right_cols.iter().position(|c| *c == right_field) {
                    Some(pos) => pos,
                    None => {
                        return Err(RSDBError::Internal(format!(
                            "Join field '{}' not found in table",
                            right_field
                        )));
                    }
                };
                // 构建哈希表
                let mut hash_map = HashMap::new();
                for row in &right_rows {
                    let rows = hash_map.entry(row[rpos].clone()).or_insert_with(Vec::new);
                    rows.push(row.clone());
                }
                // 遍历左表的行，查找匹配的右表行
                for lrow in &left_rows {
                    match hash_map.get(&lrow[lpos]) {
                        Some(rows) => {
                            for r in rows {
                                let mut row = lrow.clone();
                                row.extend(r.clone());
                                new_rows.push(row);
                            }
                        }
                        None => {
                            if self.outer {
                                let mut row = lrow.clone();
                                for _ in 0..right_cols.len() {
                                    row.push(Value::Null);
                                }
                                new_rows.push(row);
                            }
                        }
                    }
                }
                return Ok(ResultSet::Scan {
                    columns: new_cols,
                    rows: new_rows,
                });
            }
        }
        Err(RSDBError::Internal(
            "Failed to execute hash join".to_string(),
        ))
    }
}

fn parse_join_filter(predicate: Option<Expression>) -> Option<(String, String)> {
    if let Some(expr) = predicate {
        match expr {
            Expression::Field(f) => return Some((f, "".to_string())),
            Expression::Operation(operation) => match operation {
                ast::Operation::Equal(l, r) => {
                    let lv = parse_join_filter(Some(*l)).unwrap().0;
                    let rv = parse_join_filter(Some(*r)).unwrap().0;
                    return Some((lv, rv));
                }
                _ => return None,
            },
            _ => return None,
        };
    };
    None
}
