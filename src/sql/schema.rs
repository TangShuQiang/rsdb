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

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub datatype: DataType,
    pub nullable: bool,
    pub default: Option<Value>,
    pub primary_key: bool,
}
