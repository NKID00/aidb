use std::{collections::HashMap, rc::Rc};

use eyre::Result;

use crate::{Aidb, Block, BlockIndex, schema::Schema};

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
}
