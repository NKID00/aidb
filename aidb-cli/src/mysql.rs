use std::{io, sync::Arc};

use aidb_core::{Aidb, DataType, Response, Row, Value};
use async_trait::async_trait;
use futures::lock::Mutex;
use opensrv_mysql::{
    AsyncMysqlShim, Column, ColumnFlags, ColumnType, ErrorKind, InitWriter, OkResponse,
    QueryResultWriter, StatementMetaWriter, ToMysqlValue,
};
use tokio::io::AsyncWrite;
use tracing::{debug, info, trace};

#[derive(Debug, Clone)]
pub struct MySQLShim {
    pub core: Arc<Mutex<Aidb>>,
}

// error message of ER_MTS_INCONSISTENT_DATA is simply "%s"
const GENERAL_ERROR: ErrorKind = ErrorKind::ER_MTS_INCONSISTENT_DATA;

#[async_trait]
impl<W: AsyncWrite + Send + Unpin> AsyncMysqlShim<W> for MySQLShim {
    type Error = io::Error;

    fn version(&self) -> String {
        env!("CARGO_PKG_VERSION").to_owned()
    }

    fn connect_id(&self) -> u32 {
        0
    }

    async fn on_prepare<'a>(
        &'a mut self,
        _query: &'a str,
        info: StatementMetaWriter<'a, W>,
    ) -> Result<(), Self::Error> {
        debug!("prepared statement is not implmented");
        info.error(
            GENERAL_ERROR,
            "prepared statement is not implmented".as_bytes(),
        )
        .await?;
        Ok(())
    }

    async fn on_execute<'a>(
        &'a mut self,
        _id: u32,
        _params: opensrv_mysql::ParamParser<'a>,
        results: QueryResultWriter<'a, W>,
    ) -> Result<(), Self::Error> {
        debug!("prepared statement is not implmented");
        results
            .error(
                GENERAL_ERROR,
                "prepared statement is not implmented".as_bytes(),
            )
            .await?;
        Ok(())
    }

    async fn on_close(&mut self, _stmt: u32) {
        debug!("prepared statement is not implmented");
    }

    async fn on_query<'a>(
        &'a mut self,
        query: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> Result<(), Self::Error> {
        trace!(query);
        if query == "select @@version_comment limit 1" {
            let columns = [Column {
                table: "".to_owned(),
                column: "@@version_comment".to_owned(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            }];
            let mut w = results.start(&columns).await?;
            w.write_row(&["aidb"]).await?;
            return w.finish().await;
        }
        let mut lock = self.core.lock().await;
        match lock.query(query) {
            Ok(Response::Rows { columns, rows }) => {
                let columns: Vec<_> = columns.into_iter().map(aidb_type_to_mysql).collect();
                let mut r = results.start(&columns).await?;
                for row in rows {
                    r.write_row(aidb_row_to_mysql(row)).await?;
                }
                r.finish().await?;
            }
            Ok(Response::Meta { affected_rows }) => {
                results
                    .completed(OkResponse {
                        affected_rows: affected_rows as u64,
                        ..Default::default()
                    })
                    .await?;
            }
            Err(e) => {
                results
                    .error(GENERAL_ERROR, e.to_string().as_bytes())
                    .await?;
            }
        }
        Ok(())
    }

    async fn on_init<'a>(
        &'a mut self,
        database: &'a str,
        results: InitWriter<'a, W>,
    ) -> Result<(), Self::Error> {
        info!(database);
        results.ok().await?;
        Ok(())
    }
}

fn aidb_type_to_mysql(data_type: DataType) -> Column {
    Column {
        table: "".to_owned(),
        column: "".to_owned(),
        // See https://dev.mysql.com/doc/c-api/8.4/en/c-api-prepared-statement-type-codes.html
        coltype: match data_type {
            DataType::Integer => ColumnType::MYSQL_TYPE_LONGLONG,
            DataType::Real => ColumnType::MYSQL_TYPE_DOUBLE,
            DataType::Text => ColumnType::MYSQL_TYPE_BLOB,
        },
        colflags: ColumnFlags::empty(),
    }
}

#[derive(Debug)]
struct ValueWrapper(Value);

impl ToMysqlValue for ValueWrapper {
    fn to_mysql_text<W: io::Write>(&self, w: &mut W) -> io::Result<()> {
        match &self.0 {
            Value::Null => None::<u64>.to_mysql_text(w),
            Value::Integer(v) => v.to_mysql_text(w),
            Value::Real(v) => v.to_mysql_text(w),
            Value::Text(s) => s.to_mysql_text(w),
        }
    }

    fn to_mysql_bin<W: io::Write>(&self, w: &mut W, c: &Column) -> io::Result<()> {
        match &self.0 {
            Value::Null => None::<u64>.to_mysql_bin(w, c),
            Value::Integer(v) => v.to_mysql_bin(w, c),
            Value::Real(v) => v.to_mysql_bin(w, c),
            Value::Text(s) => s.to_mysql_bin(w, c),
        }
    }
}

fn aidb_row_to_mysql(row: Row) -> Vec<ValueWrapper> {
    row.into_iter().map(|v| ValueWrapper(v)).collect()
}
