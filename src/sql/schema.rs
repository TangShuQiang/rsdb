use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::{
    error::{RSDBError, RSDBResult},
    sql::types::{DataType, Row, Value},
};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

impl Table {
    // 验证表的有效性
    pub fn validate(&self) -> RSDBResult<()> {
        // 校验是否有列信息
        if self.columns.is_empty() {
            return Err(crate::error::RSDBError::Internal(format!(
                "table {} has no columns",
                self.name
            )));
        }
        // 校验是否有主键
        match self.columns.iter().filter(|c| c.primary_key).count() {
            1 => {}
            0 => {
                return Err(RSDBError::Internal(format!(
                    "No primary key for table {}",
                    self.name
                )));
            }
            _ => {
                return Err(RSDBError::Internal(format!(
                    "Multiple primary keys for table {}",
                    self.name
                )));
            }
        }
        // 校验列信息
        for col in &self.columns {
            // 主键不能为空
            if col.primary_key && col.nullable {
                return Err(RSDBError::Internal(format!(
                    "Primary key column {} in table {} cannot be nullable",
                    col.name, self.name
                )));
            }
            // 校验默认值是否与数据类型匹配
            if let Some(default_val) = &col.default {
                match default_val.datatype() {
                    Some(dt) => {
                        if dt != col.datatype {
                            return Err(RSDBError::Internal(format!(
                                "Default value for column {} in table {} does not match its datatype",
                                col.name, self.name
                            )));
                        }
                    }
                    None => {}
                }
            }
        }
        Ok(())
    }

    pub fn get_primary_key(&self, row: &Row) -> RSDBResult<Value> {
        let pos = self
            .columns
            .iter()
            .position(|c| c.primary_key)
            .expect("No primary key found");
        Ok(row[pos].clone())
    }

    pub fn get_col_index(&self, col_name: &str) -> RSDBResult<usize> {
        self.columns
            .iter()
            .position(|c| c.name == col_name)
            .ok_or(RSDBError::Internal(format!(
                "Column {} not found in table {}",
                col_name, self.name
            )))
    }
}

impl Display for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let col_desc = self
            .columns
            .iter()
            .map(|col| format!("{}", col))
            .collect::<Vec<_>>()
            .join(",\n");
        write!(f, "CREATE TABLE {} (\n{}\n)", self.name, col_desc)
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub datatype: DataType,
    pub nullable: bool,
    pub default: Option<Value>,
    pub primary_key: bool,
    pub index: bool,
}

impl Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut col_desc = format!("    {} {:?}", self.name, self.datatype);
        if self.primary_key {
            col_desc += " PRIMARY KEY";
        }
        if !self.nullable && !self.primary_key {
            col_desc += " NOT NULL";
        }
        if let Some(v) = &self.default {
            col_desc += &format!(" DEFAULT {}", v.to_string());
        }
        write!(f, "{}", col_desc)
    }
}
