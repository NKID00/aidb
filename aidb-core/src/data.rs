use std::{
    fmt::{Display, Formatter},
    io::{Cursor, Read, Write},
};

use binrw::{BinRead, BinWrite, binrw};
use eyre::{OptionExt, Result, eyre};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::{
    Aidb, Column, Response,
    schema::{IndexInfo, IndexType},
    storage::{BLOCK_SIZE, BlockIndex, BlockOffset, DataPointer},
};

#[binrw]
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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
    pub(crate) next_data_block: BlockIndex,
    #[br(map = |v: u8| v != 0u8)]
    #[bw(map = |v: &bool| if *v {1u8} else {0u8})]
    pub(crate) is_full: bool,
}

#[binrw]
#[brw(little)]
#[derive(Debug, Clone)]
pub(crate) enum ValueRepr {
    #[brw(magic = 1u8)]
    Integer(i64),
    #[brw(magic = 2u8)]
    IntegerNull(#[brw(pad_size_to = 8)] ()),
    #[brw(magic = 3u8)]
    Real(f64),
    #[brw(magic = 4u8)]
    RealNull(#[brw(pad_size_to = 8)] ()),
    #[brw(magic = 5u8)]
    Text { len: u16, ptr: DataPointer },
    #[brw(magic = 6u8)]
    TextNull(#[brw(pad_size_to = 12)] ()),
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
        let column_indices: Vec<usize> = if columns.is_empty() {
            (0..schema.columns.len()).collect()
        } else {
            let column_indices = columns
                .into_iter()
                .map(|name| {
                    schema
                        .columns
                        .iter()
                        .enumerate()
                        .find(|(_i, column)| column.name == name)
                        .map(|(i, _)| i)
                        .ok_or_eyre("column not found")
                })
                .collect::<Result<Vec<_>>>()?;
            if !column_indices.iter().all_unique() {
                return Err(eyre!("column specified multiple times"));
            }
            column_indices
        };
        let schema_columns_count = schema.columns.len();
        let schema_row_size = schema.row_size() as isize;
        let indices = &mut schema.indices;

        let mut rows = values.into_iter();
        'find_block: loop {
            let mut cursor = block.cursor();
            let mut header = DataHeader::read(&mut cursor)?;
            let mut dirty = false;
            if !header.is_full {
                while (BLOCK_SIZE as isize - cursor.position() as isize) > schema_row_size {
                    let position = cursor.position();
                    if Aidb::is_row_valid(&mut cursor)? {
                        cursor.set_position(position + schema_row_size as u64);
                        continue;
                    };
                    cursor.set_position(position);
                    let Some(row) = rows.next() else {
                        self.mark_block_dirty(index);
                        self.put_block(index, block);
                        break 'find_block;
                    };
                    let mut full_row = vec![Value::Null; schema_columns_count];
                    for item in column_indices.iter().zip_longest(row) {
                        match item {
                            itertools::EitherOrBoth::Both(i, value) => full_row[*i] = value,
                            itertools::EitherOrBoth::Left(_) => {
                                return Err(eyre!("missing values"));
                            }
                            itertools::EitherOrBoth::Right(_) => {
                                return Err(eyre!("too much values"));
                            }
                        }
                    }
                    for IndexInfo {
                        column_index,
                        type_,
                        block,
                    } in indices.iter_mut()
                    {
                        match type_ {
                            IndexType::BTree => match full_row[*column_index as usize] {
                                Value::Integer(v) => {
                                    let record = DataPointer {
                                        block: index,
                                        offset: cursor.position() as u16,
                                    };
                                    if *block == 0 {
                                        *block = self.new_btree(v, record).await?;
                                        self.mark_schema_dirty(table.clone());
                                    } else {
                                        self.insert_btree(*block, v, record).await?;
                                    }
                                }
                                Value::Null => {
                                    return Err(eyre!("indexed column must be non-null"));
                                }
                                _ => return Err(eyre!("invalid value")),
                            },
                        }
                    }
                    self.write_row(&mut cursor, &schema.columns, full_row)
                        .await?;
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

    async fn read_text(self: &mut Aidb, len: u16, ptr: DataPointer) -> Result<String> {
        if len == 0 {
            return Ok("".to_owned());
        }
        if len as usize > BLOCK_SIZE {
            return Err(eyre!("text too long"));
        }
        let mut block = self.get_block(ptr.block).await?;
        let mut cursor = block.cursor_at(ptr.offset);
        let mut buf = vec![0u8; len as usize];
        cursor.read_exact(&mut buf)?;
        self.put_block(ptr.block, block);
        Ok(String::from_utf8(buf)?)
    }

    async fn insert_text(self: &mut Aidb, s: String) -> Result<DataPointer> {
        if s.is_empty() {
            return Ok(DataPointer {
                block: 0,
                offset: 0,
            });
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
        let mut cursor = block.cursor_at(offset);
        cursor.write_all(s.as_bytes())?;
        let next_offset = cursor.position() as BlockOffset;
        self.put_block(index, block);
        self.mark_block_dirty(index);
        self.superblock.next_text_block = index;
        self.superblock.next_text_offset = next_offset;
        self.mark_superblock_dirty();
        Ok(DataPointer {
            block: index,
            offset,
        })
    }

    pub(crate) fn is_row_valid<T: AsRef<[u8]>>(cursor: &mut Cursor<T>) -> Result<bool> {
        Ok(i8::read_le(cursor)? > 0)
    }

    /// Read a row. Position of cursor may not be at row border if `Ok(None)` is returned
    pub(crate) async fn read_row<T: AsRef<[u8]>>(
        &mut self,
        cursor: &mut Cursor<T>,
    ) -> Result<Option<Vec<Value>>> {
        let position = cursor.position();
        if !Aidb::is_row_valid(cursor)? {
            return Ok(None);
        }
        cursor.set_position(position);
        debug!(position = cursor.position(), "read_row");
        let row = RowRepr::read(cursor)?;
        let mut values = vec![];
        for value in row.values {
            values.push(match value {
                ValueRepr::IntegerNull(()) | ValueRepr::RealNull(()) | ValueRepr::TextNull(()) => {
                    Value::Null
                }
                ValueRepr::Integer(v) => Value::Integer(v),
                ValueRepr::Real(v) => Value::Real(v),
                ValueRepr::Text { len, ptr } => Value::Text(self.read_text(len, ptr).await?),
            });
        }
        Ok(Some(values))
    }

    pub(crate) async fn write_row<T: AsRef<[u8]>>(
        &mut self,
        cursor: &mut Cursor<T>,
        columns: &[Column],
        row: Vec<Value>,
    ) -> Result<()>
    where
        Cursor<T>: Write,
    {
        debug!(position = cursor.position(), "write_row");
        let mut values = vec![];
        for (Column { datatype, .. }, value) in columns.iter().zip(row.into_iter()) {
            values.push(match (datatype, value) {
                (DataType::Integer, Value::Null) => ValueRepr::IntegerNull(()),
                (DataType::Real, Value::Null) => ValueRepr::RealNull(()),
                (DataType::Text, Value::Null) => ValueRepr::TextNull(()),
                (DataType::Integer, Value::Integer(v)) => ValueRepr::Integer(v),
                (DataType::Real, Value::Real(v)) => ValueRepr::Real(v),
                (DataType::Text, Value::Text(s)) => ValueRepr::Text {
                    len: s.len() as u16,
                    ptr: self.insert_text(s).await?,
                },
                _ => return Err(eyre!("invalid value")),
            });
        }
        RowRepr {
            len: values.len() as i8,
            values,
        }
        .write(cursor)?;
        Ok(())
    }
}
