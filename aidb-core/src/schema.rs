use eyre::Result;

use crate::{Response, sql::SqlColDef};

pub fn create_table(table: String, columns: Vec<SqlColDef>) -> Result<Response> {
    todo!()
}
