mod cache;
mod data;
mod query;
mod schema;
mod sql;

use std::io::{Read, Write};

use archive::{load, save};
use cache::Cache;
pub use data::{DataType, Value};
use query::dispatch;
pub use query::{Response, Row, RowStream};
use sql::complete;

pub use eyre::Result;
use opendal::Operator;

#[cfg(feature = "memory")]
use opendal::{layers::LoggingLayer, services::MemoryConfig};

#[derive(Debug)]
pub struct Aidb {
    pub(crate) op: Operator,
    pub(crate) cache: Cache,
}

impl Aidb {
    /// Create a new database with data stored in memory.
    #[cfg(feature = "memory")]
    pub fn new_memory() -> Self {
        let op = Operator::from_config(MemoryConfig::default())
            .unwrap()
            .layer(LoggingLayer::default())
            .finish();
        Self {
            op,
            cache: Cache::new(),
        }
    }

    pub fn from_op(op: Operator) -> Self {
        Self {
            op,
            cache: Cache::new(),
        }
    }

    pub async fn complete(&mut self, sql: impl AsRef<str>) -> String {
        complete(sql)
    }

    pub async fn query(&mut self, sql: impl AsRef<str>) -> Result<Response> {
        dispatch(self, sql::parse(sql)?)
    }

    pub async fn save_archive<W: Write>(&mut self, w: W) -> Result<W> {
        save(&self.op, w).await
    }

    pub async fn load_archive<R: Read>(&mut self, r: R) -> Result<R> {
        load(&self.op, r).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}
