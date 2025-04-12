#[derive(Debug, Clone)]
pub enum DataType {
    Integer,
    Real,
    Text,
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

#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Integer(u64),
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
