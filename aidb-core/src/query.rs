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
            SqlStmt::Explain {
                columns,
                table,
                join_on,
                where_,
                limit,
            } => self.explain(columns, table, join_on, where_, limit).await,
            SqlStmt::Update { table, set, where_ } => self.update(table, set, where_).await,
            SqlStmt::DeleteFrom { table, where_ } => self.delete_from(table, where_).await,
            SqlStmt::FlushTables => {
                if self.transaction_in_progress {
                    return Ok(Response::Meta { affected_rows: 0 });
                }
                self.schemas.clear();
                self.blocks.clear();
                Ok(Response::Meta { affected_rows: 0 })
            }
            SqlStmt::StartTransaction => {
                if self.transaction_in_progress {
                    return Ok(Response::Meta { affected_rows: 0 });
                }
                self.transaction_in_progress = true;
                Ok(Response::Meta { affected_rows: 0 })
            }
            SqlStmt::Commit => {
                if !self.transaction_in_progress {
                    return Ok(Response::Meta { affected_rows: 0 });
                }
                self.transaction_in_progress = false;
                Ok(Response::Meta { affected_rows: 0 })
            }
            SqlStmt::Rollback => {
                if !self.transaction_in_progress {
                    return Ok(Response::Meta { affected_rows: 0 });
                }
                self.schemas.clear();
                self.schemas_dirty.clear();
                self.blocks.clear();
                self.blocks_dirty.clear();
                self.superblock = self.superblock_backup.take().unwrap();
                self.superblock_dirty = false;
                self.transaction_in_progress = false;
                Ok(Response::Meta { affected_rows: 0 })
            }
        }
    }
}
