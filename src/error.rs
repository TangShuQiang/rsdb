use std::{array::TryFromSliceError, fmt::Display, string::FromUtf8Error, sync::PoisonError};

use bincode::ErrorKind;
use serde::{de, ser};

// 自定义 Result 类型
pub type RSDBResult<T> = std::result::Result<T, RSDBError>;

#[derive(Debug, Clone, PartialEq)]
pub enum RSDBError {
    Parse(String),
    Internal(String),
    WriteConflict,
}

impl From<std::num::ParseIntError> for RSDBError {
    fn from(value: std::num::ParseIntError) -> Self {
        RSDBError::Parse(value.to_string())
    }
}

impl From<std::num::ParseFloatError> for RSDBError {
    fn from(value: std::num::ParseFloatError) -> Self {
        RSDBError::Parse(value.to_string())
    }
}

impl<T> From<PoisonError<T>> for RSDBError {
    fn from(value: PoisonError<T>) -> Self {
        RSDBError::Internal(value.to_string())
    }
}

impl From<Box<ErrorKind>> for RSDBError {
    fn from(value: Box<ErrorKind>) -> Self {
        RSDBError::Internal(value.to_string())
    }
}

impl From<std::io::Error> for RSDBError {
    fn from(value: std::io::Error) -> Self {
        RSDBError::Internal(value.to_string())
    }
}

impl From<TryFromSliceError> for RSDBError {
    fn from(value: TryFromSliceError) -> Self {
        RSDBError::Internal(value.to_string())
    }
}

impl std::error::Error for RSDBError {}

impl ser::Error for RSDBError {
    fn custom<T: Display>(msg: T) -> Self {
        RSDBError::Internal(msg.to_string())
    }
}

impl de::Error for RSDBError {
    fn custom<T: Display>(msg: T) -> Self {
        RSDBError::Internal(msg.to_string())
    }
}

impl From<FromUtf8Error> for RSDBError {
    fn from(value: std::string::FromUtf8Error) -> Self {
        RSDBError::Internal(value.to_string())
    }
}

impl Display for RSDBError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RSDBError::Parse(err) => write!(f, "parse error: {}", err),
            RSDBError::Internal(err) => write!(f, "internal error: {}", err),
            RSDBError::WriteConflict => write!(f, "write conflict, try transaction again"),
        }
    }
}
