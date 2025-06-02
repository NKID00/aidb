mod cache;
mod data;
mod query;

use cache::Cache;
pub use data::{DataType, Value};
pub use query::{Response, Row, RowStream};

pub use eyre::Result;
use opendal::Operator;

#[derive(Debug)]
pub struct Aidb {
    pub(crate) op: Operator,
    pub(crate) cache: Cache,
}

impl Aidb {
    pub fn new(op: Operator) -> Self {
        Self {
            op,
            cache: Cache::new(),
        }
    }

    pub fn query(&mut self, query: impl AsRef<str>) -> Result<Response> {
        Ok(Response::Meta { affected_rows: 42 })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}
