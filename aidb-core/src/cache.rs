use std::{collections::HashMap, rc::Rc};

use eyre::Result;
use serde::{Deserialize, Serialize};

use crate::{Aidb, Block, BlockIndex, schema::Schema};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BlockIoLog {
    pub read: Vec<usize>,
    pub written: Vec<usize>,
}

impl Aidb {
    pub(crate) async fn new_cached_block(self: &mut Aidb) -> Option<(BlockIndex, &Block)> {
        todo!()
    }

    pub(crate) async fn get_cached_block(self: &mut Aidb, index: BlockIndex) -> Option<&Block> {
        todo!()
    }

    pub(crate) async fn ensure_block(self: &mut Aidb, index: BlockIndex) -> Option<&Block> {
        todo!()
    }

    pub(crate) async fn ensure_or_create_block(
        self: &mut Aidb,
        index: BlockIndex,
    ) -> Result<&Block> {
        todo!()
    }

    pub(crate) async fn update_cached_block(self: &mut Aidb, index: BlockIndex, block: Box<Block>) {
        todo!()
    }

    pub(crate) fn get_cached_schema(self: &mut Aidb, table: &str) -> Option<&Schema> {
        todo!()
    }

    pub(crate) fn reset_block_io_log(self: &mut Aidb) {
        todo!()
    }

    pub(crate) fn get_block_io_log(self: &mut Aidb) -> BlockIoLog {
        todo!()
    }
}
