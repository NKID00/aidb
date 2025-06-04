use eyre::Result;

use crate::{
    Aidb,
    data::{DataType, Value},
    schema::create_table,
    sql::SqlStmt,
};

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

pub fn dispatch(aidb: &mut Aidb, stmt: SqlStmt) -> Result<Response> {
    match stmt {
        SqlStmt::CreateTable { table, columns } => create_table(table, columns),
        SqlStmt::InsertInto {
            table,
            columns,
            values,
        } => todo!(),
        SqlStmt::Select {
            columns,
            table,
            join_on,
            where_,
        } => Ok(Response::Meta { affected_rows: 42 }),
        SqlStmt::Update { table, set, where_ } => todo!(),
        SqlStmt::DeleteFrom { table, where_ } => todo!(),
    }
}
