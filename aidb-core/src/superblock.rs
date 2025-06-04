use binrw::binrw;

use crate::BlockIndex;

#[binrw]
#[derive(Debug)]
#[brw(little, magic = b"aidb")]
pub struct SuperBlock {
    pub next_empty_block: BlockIndex,
    pub first_schema_block: BlockIndex,
    pub first_data_block: BlockIndex,
    pub first_journal_block: BlockIndex,
}

impl Default for SuperBlock {
    fn default() -> Self {
        Self {
            next_empty_block: 1,
            first_schema_block: 0,
            first_data_block: 0,
            first_journal_block: 0,
        }
    }
}
