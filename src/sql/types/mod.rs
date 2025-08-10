use std::{cmp::Ordering, fmt::Display, hash::Hash};

use serde::{Deserialize, Serialize};

use crate::sql::parser::ast::{Consts, Expression};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum DataType {
    Boolean,
    Integer,
    Float,
    String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}

impl Value {
    pub fn from_expression(expr: Expression) -> Self {
        match expr {
            Expression::Consts(Consts::Null) => Self::Null,
            Expression::Consts(Consts::Boolean(b)) => Self::Boolean(b),
            Expression::Consts(Consts::Integer(i)) => Self::Integer(i),
            Expression::Consts(Consts::Float(f)) => Self::Float(f),
            Expression::Consts(Consts::String(s)) => Self::String(s),
            _ => unreachable!(),
        }
    }

    pub fn datatype(&self) -> Option<DataType> {
        match self {
            Self::Null => None,
            Self::Boolean(_) => Some(DataType::Boolean),
            Self::Integer(_) => Some(DataType::Integer),
            Self::Float(_) => Some(DataType::Float),
            Self::String(_) => Some(DataType::String),
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => write!(f, "NULL"),
            Self::Boolean(b) if *b => write!(f, "TRUE"),
            Self::Boolean(_) => write!(f, "FALSE"),
            Self::Integer(i) => write!(f, "{}", i),
            Self::Float(fl) => write!(f, "{}", fl),
            Self::String(s) => write!(f, "'{}'", s),
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Self::Null, Self::Null) => Some(Ordering::Equal),
            (Self::Null, _) => Some(Ordering::Less),
            (_, Self::Null) => Some(Ordering::Greater),
            (Self::Boolean(a), Self::Boolean(b)) => a.partial_cmp(b),
            (Self::Integer(a), Self::Integer(b)) => a.partial_cmp(b),
            (Self::Integer(a), Self::Float(b)) => (*a as f64).partial_cmp(b),
            (Self::Float(a), Self::Integer(b)) => a.partial_cmp(&(*b as f64)),
            (Self::Float(a), Self::Float(b)) => a.partial_cmp(b),
            (Self::String(a), Self::String(b)) => a.partial_cmp(b),
            _ => None, // 不同类型之间不支持比较
        }
    }
}

impl Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Self::Null => state.write_u8(0),
            Self::Boolean(b) => {
                state.write_u8(1);
                b.hash(state);
            }
            Self::Integer(i) => {
                state.write_u8(2);
                i.hash(state);
            }
            Self::Float(fl) => {
                state.write_u8(3);
                fl.to_be_bytes().hash(state);
            }
            Self::String(s) => {
                state.write_u8(4);
                s.hash(state);
            }
        }
    }
}

impl Eq for Value {}

pub type Row = Vec<Value>;
