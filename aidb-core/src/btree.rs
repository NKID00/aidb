use std::ops::{Range, RangeBounds};

use binrw::binrw;
use eyre::Result;

use crate::{
    Aidb,
    storage::{BlockIndex, DataPointer},
};

#[binrw]
#[brw(little)]
#[derive(Debug)]
struct BTreeNodeHeader {
    child_count: u16,
}

#[binrw]
#[brw(little)]
#[derive(Debug)]
struct BTreeLeaveHeader {
    next_leave: BlockIndex,
    record_count: u16,
}

impl Aidb {
    async fn new_btree(&mut self) -> Result<BlockIndex> {
        todo!()
    }

    async fn insert_btree(
        &mut self,
        root: BlockIndex,
        key: i64,
        record: DataPointer,
    ) -> Result<()> {
        todo!()
    }

    async fn delete_btree(&mut self, root: BlockIndex, key: i64) -> Result<()> {
        todo!()
    }

    async fn select_btree(&mut self, root: BlockIndex, key: i64) -> Result<Option<DataPointer>> {
        Ok(self
            .select_range_btree(root, key..=key)
            .await?
            .into_iter()
            .next())
    }

    async fn select_range_btree(
        &mut self,
        root: BlockIndex,
        range: impl RangeBounds<i64>,
    ) -> Result<Vec<DataPointer>> {
        todo!()
    }
}
