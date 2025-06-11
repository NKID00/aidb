use std::{mem::swap, ops::Bound};

use binrw::{BinRead, BinWrite, binrw};
use eyre::{OptionExt, Result, eyre};

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
pub(crate) enum BTreeExactState {
    Initialized,
    Done,
}

impl Default for BTreeExactState {
    fn default() -> Self {
        Self::Initialized
    }
}

#[derive(Debug)]
pub(crate) enum BTreeRangeState {
    Initialized,
    Running {
        next: BlockIndex,
        stream: std::vec::IntoIter<(i64, DataPointer)>,
    },
}

impl Default for BTreeRangeState {
    fn default() -> Self {
        Self::Initialized
    }
}

impl Aidb {
    pub(crate) async fn new_btree(&mut self, key: i64, record: DataPointer) -> Result<BlockIndex> {
        let (leaf_i, mut leaf_b) = self.new_block();
        BTreeLeaf {
            next: 0,
            records: vec![(key, record)],
        }
        .write(&mut leaf_b.cursor())?;
        self.put_block(leaf_i, leaf_b);
        self.mark_block_dirty(leaf_i);

        let (node_i, mut node_b) = self.new_block();
        BTreeNode {
            children: vec![(leaf_i, 0)],
        }
        .write(&mut node_b.cursor())?;
        self.put_block(node_i, node_b);
        self.mark_block_dirty(node_i);

        let (root_i, mut root_b) = self.new_block();
        BTreeRoot {
            children: vec![(node_i, 0)],
        }
        .write(&mut root_b.cursor())?;
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
        if self
            .select_btree(root, key, &mut BTreeExactState::Initialized)
            .await?
            .is_some()
        {
            return Err(eyre!("unique key exists"));
        }
        self.insert_leaf(root, key, record).await
    }

    async fn read_root(&mut self, root: BlockIndex) -> Result<BTreeRoot> {
        let mut root_b = self.get_block(root).await?;
        let btree_root = BTreeRoot::read(&mut root_b.cursor())?;
        self.put_block(root, root_b);
        Ok(btree_root)
    }

    async fn write_root(&mut self, root: BlockIndex, btree_root: BTreeRoot) -> Result<()> {
        let mut root_b = self.get_block(root).await?;
        btree_root.write(&mut root_b.cursor())?;
        self.put_block(root, root_b);
        self.mark_block_dirty(root);
        Ok(())
    }

    async fn insert_root(
        &mut self,
        root: BlockIndex,
        mut key: i64,
        child: BlockIndex,
    ) -> Result<()> {
        let mut btree_root = self.read_root(root).await?;
        let mut index = btree_root.children.len() - 1;
        for (i, (_, criteria)) in btree_root.children[..btree_root.children.len() - 1]
            .iter()
            .enumerate()
        {
            if key < *criteria {
                index = i;
                break;
            }
        }
        swap(&mut btree_root.children[index].1, &mut key);
        btree_root.children.insert(index + 1, (child, key));
        self.write_root(root, btree_root).await?;
        Ok(())
    }

    async fn seek_node(&mut self, root: BlockIndex, key: i64) -> Result<BlockIndex> {
        let btree_root = self.read_root(root).await?;
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
        Ok(node_i)
    }

    async fn read_node(&mut self, node_i: BlockIndex) -> Result<BTreeNode> {
        let mut node_b = self.get_block(node_i).await?;
        let btree_node = BTreeNode::read(&mut node_b.cursor())?;
        self.put_block(node_i, node_b);
        Ok(btree_node)
    }

    async fn write_node(&mut self, node_i: BlockIndex, btree_node: BTreeNode) -> Result<()> {
        let mut node_b = self.get_block(node_i).await?;
        btree_node.write(&mut node_b.cursor())?;
        self.put_block(node_i, node_b);
        self.mark_block_dirty(node_i);
        Ok(())
    }

    async fn insert_node(
        &mut self,
        root: BlockIndex,
        mut key: i64,
        child: BlockIndex,
    ) -> Result<()> {
        let node_i = self.seek_node(root, key).await?;
        let mut btree_node = self.read_node(node_i).await?;
        let mut index = btree_node.children.len() - 1;
        for (i, (_, criteria)) in btree_node.children[..btree_node.children.len() - 1]
            .iter()
            .enumerate()
        {
            if key < *criteria {
                index = i;
                break;
            }
        }
        swap(&mut btree_node.children[index].1, &mut key);
        btree_node.children.insert(index + 1, (child, key));
        if btree_node.children.len() > BTREE_N + 1 {
            let (next_node_i, mut next_node_b) = self.new_block();
            let next_children = btree_node
                .children
                .split_off(btree_node.children.len().div_ceil(2));
            let next_key = next_children.first().unwrap().1;
            BTreeNode {
                children: next_children,
            }
            .write(&mut next_node_b.cursor())?;
            self.put_block(next_node_i, next_node_b);
            self.mark_block_dirty(next_node_i);
            self.insert_root(root, next_key, next_node_i).await?;
        }
        self.write_node(node_i, btree_node).await?;
        Ok(())
    }

    async fn seek_leaf(&mut self, root: BlockIndex, key: i64) -> Result<BlockIndex> {
        let node_i = self.seek_node(root, key).await?;
        let btree_node = self.read_node(node_i).await?;
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
        Ok(leaf_i)
    }

    async fn read_leaf(&mut self, leaf_i: BlockIndex) -> Result<BTreeLeaf> {
        let mut leaf_b = self.get_block(leaf_i).await?;
        let btree_leaf = BTreeLeaf::read(&mut leaf_b.cursor())?;
        self.put_block(leaf_i, leaf_b);
        Ok(btree_leaf)
    }

    async fn write_leaf(&mut self, leaf_i: BlockIndex, btree_leaf: BTreeLeaf) -> Result<BTreeLeaf> {
        let mut leaf_b = self.get_block(leaf_i).await?;
        btree_leaf.write(&mut leaf_b.cursor())?;
        self.put_block(leaf_i, leaf_b);
        self.mark_block_dirty(leaf_i);
        Ok(btree_leaf)
    }

    async fn insert_leaf(&mut self, root: BlockIndex, key: i64, record: DataPointer) -> Result<()> {
        let leaf_i = self.seek_leaf(root, key).await?;
        let mut btree_leaf = self.read_leaf(leaf_i).await?;
        let index = btree_leaf
            .records
            .iter()
            .position(|(criteria, _)| *criteria > key)
            .unwrap_or(btree_leaf.records.len());
        btree_leaf.records.insert(index, (key, record));
        if btree_leaf.records.len() > BTREE_N + 1 {
            let (next_leaf_i, mut next_leaf_b) = self.new_block();
            let next_records = btree_leaf
                .records
                .split_off(btree_leaf.records.len().div_ceil(2));
            let next_key = next_records.first().unwrap().0;
            BTreeLeaf {
                next: btree_leaf.next,
                records: next_records,
            }
            .write(&mut next_leaf_b.cursor())?;
            self.put_block(next_leaf_i, next_leaf_b);
            self.mark_block_dirty(next_leaf_i);
            btree_leaf.next = next_leaf_i;
            self.insert_node(root, next_key, next_leaf_i).await?;
        }
        self.write_leaf(leaf_i, btree_leaf).await?;
        Ok(())
    }

    pub(crate) async fn select_btree(
        &mut self,
        root: BlockIndex,
        key: i64,
        state: &mut BTreeExactState,
    ) -> Result<Option<DataPointer>> {
        if root == 0 {
            return Ok(None);
        }
        match state {
            BTreeExactState::Initialized => {
                let leaf_i = self.seek_leaf(root, key).await?;
                let leaf = self.read_leaf(leaf_i).await?;
                let record = leaf
                    .records
                    .into_iter()
                    .find(|(criteria, _)| key == *criteria)
                    .map(|(_, record)| record);
                *state = BTreeExactState::Done;
                Ok(record)
            }
            BTreeExactState::Done => Ok(None),
        }
    }

    pub(crate) async fn select_range_btree(
        &mut self,
        root: BlockIndex,
        range: (Bound<i64>, Bound<i64>),
        state: &mut BTreeRangeState,
    ) -> Result<Option<DataPointer>> {
        if root == 0 {
            return Ok(None);
        }
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
            BTreeRangeState::Initialized => {
                let leaf_i = self.seek_leaf(root, left_bound).await?;
                let leaf = self.read_leaf(leaf_i).await?;
                *state = BTreeRangeState::Running {
                    next: leaf.next,
                    stream: leaf.records.into_iter(),
                };
                Box::pin(self.select_range_btree(root, range, state)).await
            }
            BTreeRangeState::Running { next, stream } => {
                let mut result = vec![];
                'seek_block: loop {
                    for (criteria, record) in stream.by_ref() {
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
