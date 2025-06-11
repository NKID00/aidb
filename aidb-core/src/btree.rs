use std::ops::Bound;

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

#[derive(Debug)]
pub(crate) enum BTreeState {
    Initalized,
    Running {
        next: BlockIndex,
        stream: std::vec::IntoIter<(i64, DataPointer)>,
    },
}

impl Default for BTreeState {
    fn default() -> Self {
        Self::Initalized
    }
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

    async fn seek_leaf(&mut self, root: BlockIndex, key: i64) -> Result<BTreeLeaf> {
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

        Ok(btree_leaf)
    }

    pub(crate) async fn select_btree(
        &mut self,
        root: BlockIndex,
        key: i64,
    ) -> Result<Option<DataPointer>> {
        let leaf = self.seek_leaf(root, key).await?;
        let record = leaf
            .records
            .into_iter()
            .find(|(criteria, _)| key == *criteria)
            .map(|(_, record)| record);
        Ok(record)
    }

    pub(crate) async fn select_range_btree(
        &mut self,
        root: BlockIndex,
        range: (Bound<i64>, Bound<i64>),
        state: &mut BTreeState,
    ) -> Result<Option<DataPointer>> {
        let left_bound = match range.0 {
            Bound::Included(v) => v,
            Bound::Excluded(v) => {
                if v == i64::MAX {
                    return Ok(None);
                } else {
                    v + 1
                }
            }
            Bound::Unbounded => i64::MIN,
        };
        let right_bound = match range.0 {
            Bound::Included(v) => v,
            Bound::Excluded(v) => {
                if v == i64::MIN {
                    return Ok(None);
                } else {
                    v - 1
                }
            }
            Bound::Unbounded => i64::MAX,
        };
        match state {
            BTreeState::Initalized => {
                let leaf = self.seek_leaf(root, left_bound).await?;
                *state = BTreeState::Running {
                    next: leaf.next,
                    stream: leaf.records.into_iter(),
                };
                Box::pin(self.select_range_btree(root, range, state)).await
            }
            BTreeState::Running { next, stream } => {
                let mut result = vec![];
                'seek_block: loop {
                    while let Some((criteria, record)) = stream.next() {
                        if criteria < left_bound {
                            continue;
                        } else if criteria > right_bound {
                            break 'seek_block;
                        } else {
                            result.push(record);
                        }
                    }
                    if *next == 0 {
                        break;
                    } else {
                        let next_leaf_i = *next;
                        let mut next_leaf_b = self.get_block(next_leaf_i).await?;
                        let leaf = BTreeLeaf::read(&mut next_leaf_b.cursor())?;
                        *next = leaf.next;
                        *stream = leaf.records.into_iter();
                        self.put_block(next_leaf_i, next_leaf_b);
                    }
                }
                Ok(None)
            }
        }
    }
}
