use eyre::Result;
use serde::{Deserialize, Serialize};

use crate::{Aidb, data::Value, schema::Column, sql::SqlStmt};

pub type Row = Vec<Value>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response {
    Rows {
        columns: Vec<Column>,
        rows: Vec<Row>,
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
            SqlStmt::Update { .. } => todo!(),
            SqlStmt::DeleteFrom { .. } => todo!(),
            SqlStmt::FlushTables => {
                self.submit().await?;
                self.schemas.clear();
                self.blocks.clear();
                Ok(Response::Meta { affected_rows: 0 })
            }
        }
    }
}
