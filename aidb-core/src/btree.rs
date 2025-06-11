use std::ops::RangeBounds;

use binrw::{BinRead, BinWrite, binrw};
use eyre::{OptionExt, Result};

use crate::{
    Aidb,
    storage::{BLOCK_SIZE, BlockIndex, DataPointer},
};

const BTREE_N: usize = ((BLOCK_SIZE - 10) / 20) - 1;

#[binrw]
#[brw(little)]
#[derive(Debug)]
struct BTreeRoot {
    #[br(temp)]
    #[bw(calc = children.len() as u16)]
    len: u16,
    #[br(count = len)]
    #[bw(assert(!children.is_empty() && children.len() <= BTREE_N + 1))]
    children: Vec<(BlockIndex, i64)>,
}

#[binrw]
#[brw(little)]
#[derive(Debug)]
struct BTreeNode {
    #[br(temp)]
    #[bw(calc = children.len() as u16)]
    len: u16,
    #[br(count = len)]
    #[bw(assert(!children.is_empty() && children.len() <= BTREE_N + 1))]
    children: Vec<(BlockIndex, i64)>,
}

#[binrw]
#[brw(little)]
#[derive(Debug)]
struct BTreeLeaf {
    next: BlockIndex,
    #[br(temp)]
    #[bw(calc = records.len() as u16)]
    len: u16,
    #[br(count = len)]
    #[bw(assert(!records.is_empty() && records.len() <= BTREE_N + 1))]
    records: Vec<(i64, DataPointer)>,
}

impl Aidb {
    pub(crate) async fn new_btree(&mut self, key: i64, record: DataPointer) -> Result<BlockIndex> {
        let (root_i, mut root_b) = self.new_block();
        let (node_i, mut node_b) = self.new_block();
        let (leaf_i, mut leaf_b) = self.new_block();

        BTreeLeaf {
            next: 0,
            records: vec![(key, record)],
        }
        .write(&mut leaf_b.cursor())?;
        BTreeNode {
            children: vec![(leaf_i, 0)],
        }
        .write(&mut node_b.cursor())?;
        BTreeRoot {
            children: vec![(node_i, 0)],
        }
        .write(&mut root_b.cursor())?;

        self.put_block(leaf_i, leaf_b);
        self.mark_block_dirty(leaf_i);
        self.put_block(node_i, node_b);
        self.mark_block_dirty(node_i);
        self.put_block(root_i, root_b);
        self.mark_block_dirty(root_i);
        Ok(root_i)
    }

    pub(crate) async fn insert_btree(
        &mut self,
        root: BlockIndex,
        key: i64,
        record: DataPointer,
    ) -> Result<()> {
        todo!()
    }

    pub(crate) async fn select_btree(
        &mut self,
        root: BlockIndex,
        key: i64,
    ) -> Result<Option<DataPointer>> {
        let mut root_b = self.get_block(root).await?;
        let btree_root = BTreeRoot::read(&mut root_b.cursor())?;
        self.put_block(root, root_b);

        let mut node_i = btree_root
            .children
            .last()
            .ok_or_eyre("invalid btree index")?
            .0;
        for (child, criteria) in btree_root.children[..btree_root.children.len() - 1].iter() {
            if key < *criteria {
                node_i = *child;
                break;
            }
        }
        let mut node_b = self.get_block(node_i).await?;
        let btree_node = BTreeNode::read(&mut node_b.cursor())?;
        self.put_block(node_i, node_b);

        let mut leaf_i = btree_node
            .children
            .last()
            .ok_or_eyre("invalid btree index")?
            .0;
        for (child, criteria) in btree_node.children[..btree_node.children.len() - 1].iter() {
            if key < *criteria {
                leaf_i = *child;
                break;
            }
        }
        let mut leaf_b = self.get_block(leaf_i).await?;
        let btree_leaf = BTreeLeaf::read(&mut leaf_b.cursor())?;
        self.put_block(leaf_i, leaf_b);

        let record = btree_leaf
            .records
            .into_iter()
            .find(|(criteria, _)| key == *criteria)
            .map(|(_, record)| record);
        Ok(record)
    }

    pub(crate) async fn select_range_btree(
        &mut self,
        root: BlockIndex,
        range: impl RangeBounds<i64>,
    ) -> Result<Vec<DataPointer>> {
        todo!()
    }
}
