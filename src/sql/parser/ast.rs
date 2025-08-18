use std::collections::BTreeMap;

use crate::{
    error::{RSDBError, RSDBResult},
    sql::types::{DataType, Value},
};

// Abstract Syntax Tree 抽象语法树
#[derive(Debug, PartialEq)]
pub enum Statement {
    CreateTable {
        name: String,
        columns: Vec<Column>,
    },
    Insert {
        table_name: String,
        columns: Option<Vec<String>>,
        values: Vec<Vec<Expression>>,
    },
    Select {
        select: Vec<(Expression, Option<String>)>,
        from: FromItem,
        where_clause: Option<Expression>,
        group_by: Option<Expression>,
        having: Option<Expression>,
        order_by: Vec<(String, OrderDirection)>,
        limit: Option<Expression>,
        offset: Option<Expression>,
    },
    Update {
        table_name: String,
        columns: BTreeMap<String, Expression>,
        where_clause: Option<Expression>,
    },
    Delete {
        table_name: String,
        where_clause: Option<Expression>,
    },
    Begin,
    Commit,
    Rollback,
}

// 列定义
#[derive(Debug, PartialEq)]
pub struct Column {
    pub name: String,
    pub datatype: DataType,
    pub nullable: Option<bool>,
    pub default: Option<Expression>,
    pub primary_key: bool,
}

// 表达式定义，目前只有常量和列名
#[derive(Debug, PartialEq, Clone)]
pub enum Expression {
    Field(String), // 列名
    Consts(Consts),
    Operation(Operation),
    Function(String, String), // 聚集函数名和参数
}

impl From<Consts> for Expression {
    fn from(value: Consts) -> Self {
        Self::Consts(value)
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum Consts {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}

#[derive(Debug, PartialEq)]
pub enum OrderDirection {
    Asc,
    Desc,
}

#[derive(Debug, PartialEq)]
pub enum FromItem {
    Table {
        name: String,
    },
    Join {
        left: Box<FromItem>,
        right: Box<FromItem>,
        join_type: JoinType,
        predicate: Option<Expression>,
    },
}

#[derive(Debug, PartialEq)]
pub enum JoinType {
    Cross,
    Inner,
    Left,
    Right,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Operation {
    Equal(Box<Expression>, Box<Expression>),
    GreaterThan(Box<Expression>, Box<Expression>),
    LessThan(Box<Expression>, Box<Expression>),
}

pub fn evaluate_expr(
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

        Expression::Consts(consts) => match consts {
            Consts::Null => Ok(Value::Null),
            Consts::Boolean(b) => Ok(Value::Boolean(*b)),
            Consts::Integer(i) => Ok(Value::Integer(*i)),
            Consts::Float(f) => Ok(Value::Float(*f)),
            Consts::String(s) => Ok(Value::String(s.clone())),
        },

        Expression::Operation(operation) => match operation {
            Operation::Equal(lexpr, rexpr) => {
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
            Operation::GreaterThan(lexpr, rexpr) => {
                let lval = evaluate_expr(&lexpr, lcols, lrow, rcols, rrow)?;
                let rval = evaluate_expr(&rexpr, rcols, rrow, lcols, lrow)?;
                Ok(match (lval, rval) {
                    (Value::Boolean(l), Value::Boolean(r)) => Value::Boolean(l > r),
                    (Value::Integer(l), Value::Integer(r)) => Value::Boolean(l > r),
                    (Value::Integer(l), Value::Float(r)) => Value::Boolean(l as f64 > r),
                    (Value::Float(l), Value::Integer(r)) => Value::Boolean(l > r as f64),
                    (Value::Float(l), Value::Float(r)) => Value::Boolean(l > r),
                    (Value::String(l), Value::String(r)) => Value::Boolean(l > r),
                    (Value::Null, _) | (_, Value::Null) => Value::Null,
                    (l, r) => {
                        return Err(RSDBError::Internal(format!(
                            "Can not compare expression: {:?} and {:?}",
                            l, r
                        )));
                    }
                })
            }
            Operation::LessThan(lexpr, rexpr) => {
                let lval = evaluate_expr(&lexpr, lcols, lrow, rcols, rrow)?;
                let rval = evaluate_expr(&rexpr, rcols, rrow, lcols, lrow)?;
                Ok(match (lval, rval) {
                    (Value::Boolean(l), Value::Boolean(r)) => Value::Boolean(l < r),
                    (Value::Integer(l), Value::Integer(r)) => Value::Boolean(l < r),
                    (Value::Integer(l), Value::Float(r)) => Value::Boolean((l as f64) < r),
                    (Value::Float(l), Value::Integer(r)) => Value::Boolean(l < r as f64),
                    (Value::Float(l), Value::Float(r)) => Value::Boolean(l < r),
                    (Value::String(l), Value::String(r)) => Value::Boolean(l < r),
                    (Value::Null, _) | (_, Value::Null) => Value::Null,
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
