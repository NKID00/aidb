use std::{collections::HashSet, io::Cursor, mem::swap};

use binrw::BinWrite;
use eyre::Result;
use serde::{Deserialize, Serialize};
use tracing::{error, warn};

use crate::Aidb;

pub type BlockIndex = u64;

pub const BLOCK_SIZE: usize = 8 * 1024;

#[derive(Debug)]
pub struct Block(Box<[u8; BLOCK_SIZE]>);

impl Block {
    pub(crate) fn cursor(&mut self) -> Cursor<&mut [u8]> {
        Cursor::new(self.0.as_mut_slice())
    }

    pub(crate) fn cursor_at(&mut self, offset: usize) -> Cursor<&mut [u8]> {
        let mut cursor = self.cursor();
        cursor.set_position(offset as u64);
        cursor
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BlockIoLog {
    pub read: HashSet<BlockIndex>,
    pub written: HashSet<BlockIndex>,
}

impl Aidb {
    pub(crate) async fn new_block(self: &mut Aidb) -> (BlockIndex, Block) {
        let index = self.superblock.next_empty_block;
        self.superblock.next_empty_block += 1;
        self.mark_superblock_dirty();
        (index, Self::new_volatile_block())
    }

    pub(crate) async fn get_block(self: &mut Aidb, index: BlockIndex) -> Result<Block> {
        if let Some(b) = self.blocks.remove(&index) {
            return Ok(b);
        }
        Ok(self.read_physical(index).await?)
    }

    pub(crate) fn put_block(self: &mut Aidb, index: BlockIndex, block: Block) {
        self.blocks.insert(index, block);
    }

    pub(crate) fn mark_block_dirty(self: &mut Aidb, index: BlockIndex) {
        self.blocks_dirty.insert(index);
    }

    pub(crate) async fn submit(self: &mut Aidb) -> Result<()> {
        if self.superblock_dirty {
            let mut block = Self::new_volatile_block();
            self.superblock.write(&mut block.cursor()).unwrap();
            self.put_block(0, block);
            self.mark_block_dirty(0);
            self.superblock_dirty = false;
        }

        let mut schemas_dirty = HashSet::new();
        swap(&mut self.schemas_dirty, &mut schemas_dirty);
        for table in schemas_dirty {
            let schema = self.get_schema(&table).await.unwrap();
            self.save_schema(&schema).await?;
            self.put_schema(table, schema);
        }

        let mut blocks_dirty = HashSet::new();
        swap(&mut self.blocks_dirty, &mut blocks_dirty);
        for index in blocks_dirty {
            let block = self.get_block(index).await.unwrap();
            self.write_physical(index, &block).await?;
            self.put_block(index, block);
        }

        Ok(())
    }

    pub fn new_volatile_block() -> Block {
        Block(vec![0; BLOCK_SIZE].into_boxed_slice().try_into().unwrap())
    }

    pub async fn read_physical(&mut self, index: BlockIndex) -> opendal::Result<Block> {
        let buffer = self.op.read(&index.to_string()).await?;
        let mut v = buffer.to_vec();
        if v.len() < BLOCK_SIZE {
            warn!("file size is smaller than block size, padding with zero");
        } else if v.len() > BLOCK_SIZE {
            error!("file size is larger than block size, truncating");
        }
        v.resize(BLOCK_SIZE, 0);
        let block = Block(v.into_boxed_slice().try_into().unwrap());
        self.log.read.insert(index);
        Ok(block)
    }

    pub async fn write_physical(
        &mut self,
        index: BlockIndex,
        block: &Block,
    ) -> opendal::Result<()> {
        self.op.write(&index.to_string(), block.0.to_vec()).await?;
        self.log.written.insert(index);
        Ok(())
    }

    pub(crate) fn reset_block_io_log(self: &mut Aidb) {
        self.log = BlockIoLog::default();
    }

    pub(crate) fn get_block_io_log(self: &mut Aidb) -> BlockIoLog {
        self.log.clone()
    }
}
