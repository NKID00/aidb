use binrw::{BinRead, binrw};
use eyre::Result;
use opendal::ErrorKind;

use crate::{Aidb, BlockIndex, storage::BlockOffset};

#[binrw]
#[derive(Debug, Clone)]
#[brw(little, magic = b"aidb")]
pub struct SuperBlock {
    pub(crate) next_empty_block: BlockIndex,
    pub(crate) first_schema_block: BlockIndex,
    pub(crate) first_journal_block: BlockIndex,
    pub(crate) next_text_block: BlockIndex,
    pub(crate) next_text_offset: BlockOffset,
}

impl Default for SuperBlock {
    fn default() -> Self {
        Self {
            next_empty_block: 1,
            first_schema_block: 0,
            first_journal_block: 0,
            next_text_block: 0,
            next_text_offset: 0,
        }
    }
}

impl Aidb {
    pub(crate) async fn load_superblock(self: &mut Aidb) -> Result<()> {
        match self.read_physical(0).await {
            Ok(mut block) => {
                let mut cursor = block.cursor();
                self.superblock = SuperBlock::read(&mut cursor)?;
            }
            Err(e) if e.kind() == ErrorKind::NotFound => {
                self.mark_superblock_dirty();
            }
            Err(e) => Err(e)?,
        }
        Ok(())
    }

    pub(crate) fn mark_superblock_dirty(self: &mut Aidb) {
        self.superblock_dirty = true
    }
}
