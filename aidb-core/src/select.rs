use crate::{
    Aidb, Response,
    sql::{SqlOn, SqlSelectTarget, SqlWhere},
};

use eyre::Result;

impl Aidb {
    pub(crate) async fn select(
        &mut self,
        columns: Vec<SqlSelectTarget>,
        table: Option<String>,
        join_on: Vec<(String, SqlOn)>,
        where_: Option<SqlWhere>,
        limit: Option<u64>,
    ) -> Result<Response> {
        Ok(Response::Meta { affected_rows: 42 })
    }
}
