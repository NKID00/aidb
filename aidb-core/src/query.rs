use eyre::Result;

use crate::{Aidb, data::Value, schema::Column, sql::SqlStmt};

pub type Row = Vec<Value>;

pub struct RowStream(pub(crate) Box<dyn Iterator<Item = Row> + Send>);

impl Iterator for RowStream {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

pub enum Response {
    Rows {
        columns: Vec<Column>,
        rows: RowStream,
    },
    Meta {
        affected_rows: usize,
    },
}

impl Aidb {
    pub async fn dispatch(self: &mut Aidb, stmt: SqlStmt) -> Result<Response> {
        match stmt {
            SqlStmt::ShowTables => self.show_tables().await,
            SqlStmt::Describe { table } => self.describe(table).await,
            SqlStmt::CreateTable { table, columns } => self.create_table(table, columns).await,
            SqlStmt::InsertInto {
                table,
                columns,
                values,
            } => self.insert_into(table, columns, values).await,
            SqlStmt::Select {
                columns,
                table,
                join_on,
                where_,
                limit,
            } => self.select(columns, table, join_on, where_, limit).await,
            SqlStmt::Update { table, set, where_ } => todo!(),
            SqlStmt::DeleteFrom { table, where_ } => todo!(),
        }
    }
}
