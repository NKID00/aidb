use crate::data::{DataType, Value};

pub type Row = Vec<Value>;

#[derive(Debug)]
pub struct RowStream;

impl Iterator for RowStream {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

#[derive(Debug)]
pub enum Response {
    Rows {
        columns: Vec<(String, DataType)>,
        rows: RowStream,
    },
    Meta {
        affected_rows: usize,
    },
}
