use std::fmt::{Display, Formatter};

use binrw::binrw;
use eyre::Result;
use serde::{Deserialize, Serialize};

use crate::{Aidb, Response, sql::SqlCol};

#[binrw]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[brw(little, repr = u8)]
pub enum DataType {
    Integer = 1,
    Real = 2,
    Text = 3,
}

impl DataType {
    pub fn default_value(&self) -> Value {
        match self {
            DataType::Integer => Value::Integer(0),
            DataType::Real => Value::Real(0f64),
            DataType::Text => Value::Text("".to_owned()),
        }
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Integer => write!(f, "INTEGER"),
            DataType::Real => write!(f, "REAL"),
            DataType::Text => write!(f, "TEXT"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
}

impl Value {
    pub fn data_type(&self) -> Option<DataType> {
        match self {
            Value::Null => None,
            Value::Integer(_) => Some(DataType::Integer),
            Value::Real(_) => Some(DataType::Real),
            Value::Text(_) => Some(DataType::Text),
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Integer(v) => write!(f, "{v}"),
            Value::Real(v) => write!(f, "{v}"),
            Value::Text(v) => write!(f, "{}", v.escape_debug()),
        }
    }
}

impl Aidb {
    pub(crate) async fn insert_into(
        &mut self,
        table: String,
        columns: Vec<SqlCol>,
        values: Vec<Vec<Value>>,
    ) -> Result<Response> {
        todo!()
    }
}
