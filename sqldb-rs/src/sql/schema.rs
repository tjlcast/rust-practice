use serde::{Deserialize, Serialize};

use crate::{
    error::{Error, Result},
    sql::types::{DataType, Row, Value},
};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

impl Table {
    // 校验表的有效性
    pub fn validate(&self) -> Result<()> {
        if self.columns.is_empty() {
            // 校验是否有列信息
            return Err(Error::Internal(format!(
                "table {} has no columns",
                self.name
            )));
        }

        // 检查是否有主键
        match self.columns.iter().filter(|c| c.primary_key).count() {
            1 => {}
            0 => {
                return Err(Error::Internal(format!(
                    "No primary key found for table {}",
                    self.name
                )));
            }
            _ => {
                return Err(Error::Internal(format!(
                    "Multiple primary keys found for table {}",
                    self.name
                )));
            }
        }

        Ok(())
    }

    pub fn get_primary_key(&self, row: &Row) -> Result<Value> {
        let position = self
            .columns
            .iter()
            .position(|c| c.primary_key)
            .expect("No primary key found");

        Ok(row[position].clone())
    }
}

// 关联到 Plan
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub datatype: DataType,
    pub nullable: bool,
    pub default: Option<Value>,
    pub primary_key: bool,
}
