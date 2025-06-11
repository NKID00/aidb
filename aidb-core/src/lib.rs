mod btree;
mod data;
mod query;
mod schema;
mod select;
mod sql;
mod storage;
mod superblock;

use std::{
    collections::{HashMap, HashSet},
    io::{Read, Write},
};

pub use data::{DataType, Value};
pub use query::{Response, Row};
pub use schema::Column;
pub use storage::BlockIoLog;

use archive::{load, save};
use schema::Schema;
use storage::{Block, BlockIndex};
use superblock::SuperBlock;

pub use eyre::Result;
use opendal::Operator;

#[cfg(feature = "memory")]
use opendal::{layers::LoggingLayer, services::MemoryConfig};

#[derive(Debug)]
pub struct Aidb {
    pub(crate) op: Operator,
    pub(crate) log: BlockIoLog,
    pub(crate) blocks: HashMap<BlockIndex, Block>,
    pub(crate) blocks_dirty: HashSet<BlockIndex>,
    pub(crate) superblock: SuperBlock,
    pub(crate) superblock_dirty: bool,
    pub(crate) schemas: HashMap<String, Box<Schema>>,
    pub(crate) schemas_dirty: HashSet<String>,
    pub(crate) transaction_in_progress: bool,
    pub(crate) superblock_backup: Option<SuperBlock>,
}

impl Aidb {
    /// Create a new database with data stored in memory.
    #[cfg(feature = "memory")]
    pub async fn new_memory() -> Self {
        let op = Operator::from_config(MemoryConfig::default())
            .unwrap()
            .layer(LoggingLayer::default())
            .finish();
        let mut this = Self {
            op,
            log: BlockIoLog::default(),
            blocks: HashMap::new(),
            blocks_dirty: HashSet::new(),
            superblock: SuperBlock::default(),
            superblock_dirty: true,
            schemas: HashMap::new(),
            schemas_dirty: HashSet::new(),
            transaction_in_progress: false,
            superblock_backup: None,
        };
        this.submit().await.unwrap();
        this
    }

    pub async fn from_op(op: Operator) -> Result<Self> {
        let mut this = Self {
            op,
            log: BlockIoLog::default(),
            blocks: HashMap::new(),
            blocks_dirty: HashSet::new(),
            superblock: SuperBlock::default(),
            superblock_dirty: false,
            schemas: HashMap::new(),
            schemas_dirty: HashSet::new(),
            transaction_in_progress: false,
            superblock_backup: None,
        };
        this.load_superblock().await?;
        this.submit().await?;
        Ok(this)
    }

    pub async fn query(&mut self, sql: impl AsRef<str>) -> Result<Response> {
        self.superblock_backup = Some(self.superblock.clone());
        let r = self.dispatch(Self::parse(sql)?).await;
        if r.is_ok() {
            if !self.transaction_in_progress {
                self.submit().await?;
            }
        } else {
            self.transaction_in_progress = true;
            self.dispatch(sql::SqlStmt::Rollback).await.unwrap();
        }
        r
    }

    pub async fn query_log_blocks(
        &mut self,
        sql: impl AsRef<str>,
    ) -> Result<(Response, BlockIoLog)> {
        self.reset_block_io_log();
        let result = self.query(sql).await;
        result.map(|r| (r, self.get_block_io_log()))
    }

    pub async fn save_archive<W: Write>(&mut self, w: W) -> Result<W> {
        save(&self.op, w).await
    }

    pub async fn load_archive<R: Read>(&mut self, r: R) -> Result<R> {
        load(&self.op, r).await
    }
}
