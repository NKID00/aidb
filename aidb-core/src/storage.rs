use opendal::{Operator, Result};
use tracing::{error, warn};

use crate::{Aidb, BLOCK_SIZE, Block};

impl Aidb {
    pub fn new_memory_block() -> Box<Block> {
        vec![0; BLOCK_SIZE].into_boxed_slice().try_into().unwrap()
    }

    pub async fn read(&mut self, index: u64) -> Result<Box<Block>> {
        let buffer = self.op.read(&format!("{}", index)).await?;
        let mut v = buffer.to_vec();
        if v.len() < BLOCK_SIZE {
            warn!("file size is smaller than block size, padding with zero");
        } else if v.len() > BLOCK_SIZE {
            error!("file size is larger than block size, truncating");
        }
        v.resize(BLOCK_SIZE, 0);
        Ok(v.into_boxed_slice().try_into().unwrap())
    }

    pub async fn write(&mut self, index: u64, block: &Block) -> Result<()> {
        self.op.write(&format!("{}", index), block.to_vec()).await?;
        Ok(())
    }
}
