use crate::data::{DataType, Value};

pub type Row = Vec<Value>;

#[derive(Debug)]
pub struct Rows;

impl Iterator for Rows {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

#[derive(Debug)]
pub enum Response {
    Rows { columns: Vec<DataType>, rows: Rows },
    Meta { affected_rows: usize },
}
