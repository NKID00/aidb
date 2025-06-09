use std::{
    fmt::{Display, Formatter},
    io::{Cursor, Write},
};

use binrw::{BinRead, BinWrite, binrw};
use eyre::{OptionExt, Result, eyre};
use serde::{Deserialize, Serialize};
use tracing::trace;

use crate::{
    Aidb, Response,
    storage::{BLOCK_SIZE, BlockIndex, BlockOffset},
};

#[binrw]
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[brw(little, repr = u8)]
pub enum DataType {
    Integer = 1,
    Real = 2,
    Text = 3,
}

impl DataType {
    pub fn default_value(&self) -> Value {
        match self {
            DataType::Integer => Value::Integer(0),
            DataType::Real => Value::Real(0f64),
            DataType::Text => Value::Text("".to_owned()),
        }
    }

    pub fn size(&self) -> usize {
        match self {
            DataType::Integer => size_of::<u64>(),
            DataType::Real => size_of::<f64>(),
            DataType::Text => size_of::<u64>() + size_of::<u64>(),
        }
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Integer => write!(f, "INTEGER"),
            DataType::Real => write!(f, "REAL"),
            DataType::Text => write!(f, "TEXT"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
}

impl Value {
    pub fn datatype(&self) -> Option<DataType> {
        match self {
            Value::Null => None,
            Value::Integer(_) => Some(DataType::Integer),
            Value::Real(_) => Some(DataType::Real),
            Value::Text(_) => Some(DataType::Text),
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Integer(v) => write!(f, "{v}"),
            Value::Real(v) => write!(f, "{v}"),
            Value::Text(v) => write!(f, "{}", v.escape_debug()),
        }
    }
}

#[binrw]
#[brw(little)]
#[derive(Debug, Clone)]
pub(crate) struct DataHeader {
    next_data_block: BlockIndex,
    #[br(map = |v: u8| v != 0u8)]
    #[bw(map = |v: &bool| if *v {1u8} else {0u8})]
    is_full: bool,
}

#[binrw]
#[brw(little)]
#[derive(Debug, Clone)]
pub(crate) enum ValueRepr {
    #[brw(magic = 0u8)]
    NumNull(#[brw(pad_size_to = 8)] ()),
    #[brw(magic = 1u8)]
    Integer(i64),
    #[brw(magic = 2u8)]
    Real(f64),
    #[brw(magic = 0u8)]
    TextNull(#[brw(pad_size_to = 18)] ()),
    #[brw(magic = 3u8)]
    Text {
        len: u64,
        block: BlockIndex,
        offset: BlockOffset,
    },
}

impl ValueRepr {
    pub(crate) fn is_null(&self) -> bool {
        matches!(self, ValueRepr::NumNull(()) | ValueRepr::TextNull(()))
    }

    pub(crate) fn is_integer(&self) -> bool {
        matches!(self, ValueRepr::Integer(_))
    }

    pub(crate) fn is_real(&self) -> bool {
        matches!(self, ValueRepr::Real(_))
    }

    pub(crate) fn is_text(&self) -> bool {
        matches!(self, ValueRepr::Text { .. })
    }
}

#[binrw]
#[brw(little)]
#[bw(assert(*len == 0 || len.unsigned_abs() as usize == values.len()))]
#[derive(Debug, Clone)]
pub(crate) struct RowRepr {
    len: i8,
    #[br(count = len.abs())]
    values: Vec<ValueRepr>,
}

impl Aidb {
    pub(crate) async fn insert_into(
        &mut self,
        table: String,
        columns: Vec<String>,
        values: Vec<Vec<Value>>,
    ) -> Result<Response> {
        let mut schema = self.get_schema(&table).await?;
        let affected_rows = values.len();
        let (mut index, mut block) = if schema.data_block == 0 {
            let (index, block) = self.new_block();
            schema.data_block = index;
            self.mark_schema_dirty(table.clone());
            (index, block)
        } else {
            (schema.data_block, self.get_block(schema.data_block).await?)
        };
        let columns: Vec<(usize, DataType)> = columns
            .into_iter()
            .map(|name| {
                schema
                    .columns
                    .iter()
                    .enumerate()
                    .find(|(_i, column)| column.name == name)
                    .map(|(i, column)| (i, column.datatype))
                    .ok_or_eyre("column not found")
            })
            .collect::<Result<Vec<_>>>()?;
        let mut rows = values.into_iter();
        let schema_row_size = schema.row_size() as isize;
        'find_block: loop {
            let mut cursor = block.cursor();
            let mut header = DataHeader::read(&mut cursor)?;
            let mut dirty = false;
            if !header.is_full {
                while (BLOCK_SIZE as isize - cursor.position() as isize) > schema_row_size {
                    let position = cursor.position();
                    let row = RowRepr::read(&mut cursor)?;
                    if row.len > 0 {
                        continue;
                    }
                    cursor.set_position(position);
                    let Some(row) = rows.next() else {
                        self.mark_block_dirty(index);
                        self.put_block(index, block);
                        break 'find_block;
                    };
                    let mut row_repr: Vec<_> = schema
                        .columns
                        .iter()
                        .map(|column| match column.datatype {
                            DataType::Integer | DataType::Real => ValueRepr::NumNull(()),
                            DataType::Text => ValueRepr::TextNull(()),
                        })
                        .collect();
                    for ((i, datatype), value) in columns.iter().zip(row.into_iter()) {
                        if !matches!(value, Value::Null) {
                            row_repr[*i] = match (datatype, value) {
                                (DataType::Integer, Value::Integer(v)) => ValueRepr::Integer(v),
                                (DataType::Real, Value::Real(v)) => ValueRepr::Real(v),
                                (DataType::Text, Value::Text(s)) => {
                                    let len = s.len() as u64;
                                    let (block, offset) = self.insert_text(s).await?;
                                    ValueRepr::Text { len, block, offset }
                                }
                                _ => return Err(eyre!("invalid value")),
                            };
                        }
                    }
                    Aidb::write_row(&mut cursor, row_repr)?;
                }
                dirty = true;
                header.is_full = true;
            }
            let (next_index, next_block) = if header.next_data_block == 0 {
                let (next_index, next_block) = self.new_block();
                header.next_data_block = next_index;
                dirty = true;
                (next_index, next_block)
            } else {
                (
                    header.next_data_block,
                    self.get_block(header.next_data_block).await?,
                )
            };
            cursor.set_position(0);
            header.write(&mut cursor)?;
            self.put_block(index, block);
            if dirty {
                self.mark_block_dirty(index);
            }
            (index, block) = (next_index, next_block);
        }
        self.put_schema(table, schema);
        Ok(Response::Meta { affected_rows })
    }

    async fn insert_text(self: &mut Aidb, s: String) -> Result<(BlockIndex, BlockOffset)> {
        if s.is_empty() {
            return Ok((0, 0));
        }
        if s.len() > BLOCK_SIZE {
            return Err(eyre!("text too long"));
        }
        let ((index, mut block), offset) = if self.superblock.next_text_block == 0
            || (BLOCK_SIZE - self.superblock.next_text_offset as usize) < s.len()
        {
            (self.new_block(), 0)
        } else {
            let index = self.superblock.next_text_block;
            (
                (index, self.get_block(index).await?),
                self.superblock.next_text_offset,
            )
        };
        let mut cursor = block.cursor_at(offset as usize);
        cursor.write_all(s.as_bytes())?;
        let offset = cursor.position() as u16;
        self.put_block(index, block);
        self.mark_block_dirty(index);
        self.superblock.next_text_block = index;
        self.superblock.next_text_offset = offset;
        self.mark_superblock_dirty();
        Ok((index, offset))
    }

    pub(crate) fn read_row<T: AsRef<[u8]>>(
        cursor: &mut Cursor<T>,
    ) -> Result<Option<Vec<ValueRepr>>> {
        let row = RowRepr::read(cursor)?;
        if row.len <= 0 {
            Ok(None)
        } else {
            Ok(Some(row.values))
        }
    }

    pub(crate) fn write_row<T: AsRef<[u8]>>(
        cursor: &mut Cursor<T>,
        row: Vec<ValueRepr>,
    ) -> Result<()>
    where
        Cursor<T>: Write,
    {
        RowRepr {
            len: row.len() as i8,
            values: row,
        }
        .write(cursor)?;
        Ok(())
    }
}
