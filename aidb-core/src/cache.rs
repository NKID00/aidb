use std::{collections::HashMap, io::Cursor, rc::Rc};

use binrw::BinWrite;
use eyre::Result;

use crate::{Aidb, Block, BlockIndex, schema::Schema};

impl Aidb {
    pub(crate) async fn write_superblock(self: &mut Aidb) -> Result<()> {
        let mut block = Self::new_memory_block();
        let mut cursor = Cursor::new(block.as_mut_slice());
        self.superblock.write(&mut cursor).unwrap();
        self.update_cached_block(0, block);
        self.write_block(0).await?;
        Ok(())
    }

    pub(crate) async fn new_cached_block(self: &mut Aidb) -> Option<(BlockIndex, Box<Block>)> {
        let index = self.superblock.next_empty_block;
        self.superblock.next_empty_block += 1;
        self.write_superblock().await.ok()?;
        self.ensure_or_create_block(index)
            .await
            .ok()
            .map(|b| (index, b))
    }

    pub(crate) fn get_cached_block(self: &mut Aidb, index: BlockIndex) -> Option<Box<Block>> {
        self.cache_block.remove(&index)
    }

    pub(crate) async fn ensure_block(self: &mut Aidb, index: BlockIndex) -> Option<Box<Block>> {
        if let Some(b) = self.get_cached_block(index) {
            return Some(b);
        }
        self.read_block(index).await.ok()?;
        self.get_cached_block(index)
    }

    pub(crate) async fn ensure_or_create_block(
        self: &mut Aidb,
        index: BlockIndex,
    ) -> Result<Box<Block>> {
        if let Some(b) = self.ensure_block(index).await {
            return Ok(b);
        }
        let block = Self::new_memory_block();
        self.write(index, &block).await?;
        Ok(block)
    }

    pub(crate) async fn read_block(self: &mut Aidb, index: BlockIndex) -> Result<()> {
        let block = self.read(index).await?;
        self.cache_block.insert(index, block);
        Ok(())
    }

    pub(crate) fn update_cached_block(self: &mut Aidb, index: BlockIndex, block: Box<Block>) {
        self.cache_block.insert(index, block);
    }

    pub(crate) async fn write_block(self: &mut Aidb, index: BlockIndex) -> Result<()> {
        if let Some(b) = self.get_cached_block(index) {
            self.write(index, &b).await?;
            self.update_cached_block(index, b);
        }
        Ok(())
    }

    pub(crate) async fn get_cached_schema(self: &mut Aidb, table: &str) -> Option<&Schema> {
        todo!()
    }
}
