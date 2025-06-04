mod cache;
mod data;
mod query;
mod schema;
mod sql;
mod storage;
mod superblock;

use std::{
    collections::HashMap,
    io::{Cursor, Read, Write},
};

pub use data::{DataType, Value};
pub use query::{Response, Row, RowStream};
pub use schema::Column;

use archive::{load, save};
use superblock::SuperBlock;

use binrw::{BinRead, BinWrite};
pub use eyre::Result;
use opendal::{ErrorKind, Operator};

#[cfg(feature = "memory")]
use opendal::{layers::LoggingLayer, services::MemoryConfig};

use crate::schema::Schema;

type BlockIndex = u64;

const BLOCK_SIZE: usize = 8 * 1024;
type Block = [u8; BLOCK_SIZE];

#[derive(Debug)]
pub struct Aidb {
    pub(crate) op: Operator,
    pub(crate) superblock: SuperBlock,
    pub(crate) cache_block: HashMap<BlockIndex, Box<Block>>,
    pub(crate) cache_schema: HashMap<String, Box<Schema>>,
}

impl Aidb {
    /// Create a new database with data stored in memory.
    #[cfg(feature = "memory")]
    pub async fn new_memory() -> Self {
        let op = Operator::from_config(MemoryConfig::default())
            .unwrap()
            .layer(LoggingLayer::default())
            .finish();
        let superblock = SuperBlock::default();
        let mut block = Self::new_memory_block();
        let mut cursor = Cursor::new(block.as_mut_slice());
        superblock.write(&mut cursor).unwrap();
        let mut this = Self {
            op,
            superblock,
            cache_block: HashMap::new(),
            cache_schema: HashMap::new(),
        };
        this.write(0, &block).await.unwrap();
        this
    }

    pub async fn from_op(op: Operator) -> Result<Self> {
        let mut this = Self {
            op,
            superblock: SuperBlock::default(),
            cache_block: HashMap::new(),
            cache_schema: HashMap::new(),
        };
        match this.read(0).await {
            Ok(block) => {
                let mut cursor = Cursor::new(block.as_slice());
                this.superblock = SuperBlock::read(&mut cursor)?;
            }
            Err(e) if e.kind() == ErrorKind::NotFound => {
                let mut block = Self::new_memory_block();
                let mut cursor = Cursor::new(block.as_mut_slice());
                this.superblock.write(&mut cursor)?;
                this.write(0, &block).await?;
            }
            Err(e) => Err(e)?,
        }
        Ok(this)
    }

    pub async fn query(&mut self, sql: impl AsRef<str>) -> Result<Response> {
        self.dispatch(Self::parse(sql)?).await
    }

    pub async fn save_archive<W: Write>(&mut self, w: W) -> Result<W> {
        save(&self.op, w).await
    }

    pub async fn load_archive<R: Read>(&mut self, r: R) -> Result<R> {
        load(&self.op, r).await
    }
}
