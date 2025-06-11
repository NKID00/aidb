use binrw::{BinRead, BinWrite, binrw};
use eyre::{Result, eyre};
use serde::{Deserialize, Serialize};

use crate::{Aidb, BlockIndex, DataType, Response, Value};

#[binrw]
#[brw(little, repr = u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexType {
    BTree = 1,
}

#[binrw]
#[brw(little)]
#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub column_index: u8,
    pub type_: IndexType,
    pub block: BlockIndex,
}

#[binrw]
#[brw(little)]
#[derive(Debug, Clone)]
pub struct Schema {
    #[brw(ignore)]
    block_index: BlockIndex,
    next_schema_block: BlockIndex,
    #[br(temp)]
    #[bw(calc = name.len() as u8)]
    name_len: u8,
    #[br(count = name_len, try_map = |s: Vec<u8>| String::from_utf8(s))]
    #[bw(map = |s: &String| s.as_bytes())]
    name: String,
    #[br(temp)]
    #[bw(calc = columns.len() as u8)]
    columns_len: u8,
    #[br(count = columns_len)]
    pub(crate) columns: Vec<Column>,
    #[br(temp)]
    #[bw(calc = indices.len() as u8)]
    indices_len: u8,
    #[br(count = indices_len)]
    pub(crate) indices: Vec<IndexInfo>,
    pub(crate) data_block: BlockIndex,
}

impl Schema {
    pub(crate) fn row_size(&self) -> usize {
        1 + self
            .columns
            .iter()
            .map(|column| match column.datatype {
                DataType::Integer => 9,
                DataType::Real => 9,
                DataType::Text => 13,
            })
            .sum::<usize>()
    }
}

#[binrw]
#[brw(little)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    #[br(temp)]
    #[bw(calc = name.len() as u64)]
    name_len: u64,
    #[br(count = name_len, try_map = |s: Vec<u8>| String::from_utf8(s))]
    #[bw(map = |s: &String| s.as_bytes())]
    pub name: String,
    pub datatype: DataType,
}

impl Aidb {
    pub async fn show_tables(self: &mut Aidb) -> Result<Response> {
        let mut schema_block_index = self.superblock.first_schema_block;
        let mut tables = vec![];
        while schema_block_index > 0 {
            let mut block = self.get_block(schema_block_index).await?;
            let mut schema = Schema::read(&mut block.cursor())?;
            schema.block_index = schema_block_index;
            tables.push(schema.name.clone());
            self.put_block(schema_block_index, block);
            let next_schema_block_index = schema.next_schema_block;
            self.put_schema(schema.name.clone(), Box::new(schema));
            schema_block_index = next_schema_block_index;
        }
        Ok(Response::Rows {
            columns: vec![Column {
                name: "table_name".to_owned(),
                datatype: DataType::Text,
            }],
            rows: tables.into_iter().map(|s| vec![Value::Text(s)]).collect(),
        })
    }

    pub async fn describe(self: &mut Aidb, table: String) -> Result<Response> {
        let schema = self.get_schema(&table).await?;
        let r = Response::Rows {
            columns: vec![
                Column {
                    name: "column_name".to_owned(),
                    datatype: DataType::Text,
                },
                Column {
                    name: "column_datatype".to_owned(),
                    datatype: DataType::Text,
                },
            ],
            rows: schema
                .columns
                .iter()
                .map(|column| {
                    vec![
                        Value::Text(column.name.clone()),
                        Value::Text(column.datatype.to_string()),
                    ]
                })
                .collect(),
        };
        self.put_schema(table, schema);
        Ok(r)
    }

    async fn new_schema_block(
        &mut self,
        table: String,
        columns: Vec<Column>,
        indices: Vec<IndexInfo>,
    ) -> Result<BlockIndex> {
        let (index, mut block) = self.new_block();
        let schema = Schema {
            block_index: index,
            next_schema_block: 0,
            name: table.clone(),
            columns,
            indices,
            data_block: 0,
        };
        schema.write(&mut block.cursor())?;
        self.put_schema(table.clone(), Box::new(schema));
        self.mark_schema_dirty(table);
        self.put_block(index, block);
        self.mark_block_dirty(index);
        Ok(index)
    }

    pub async fn create_table(
        self: &mut Aidb,
        table: String,
        columns: Vec<(Column, Option<IndexType>)>,
    ) -> Result<Response> {
        let mut schema_columns = vec![];
        let mut schema_indices = vec![];
        for (i, (column, index)) in columns.into_iter().enumerate() {
            if let Some(type_) = index {
                if column.datatype != DataType::Integer {
                    return Err(eyre!("index is implemented on integer column only"));
                }
                schema_indices.push(IndexInfo {
                    column_index: i as u8,
                    type_,
                    block: 0,
                });
            }
            schema_columns.push(column);
        }

        let mut schema_block_index = self.superblock.first_schema_block;
        if schema_block_index == 0 {
            let index = self
                .new_schema_block(table, schema_columns, schema_indices)
                .await?;
            self.superblock.first_schema_block = index;
            self.mark_superblock_dirty();
            return Ok(Response::Meta { affected_rows: 0 });
        }
        loop {
            let mut block = self.get_block(schema_block_index).await?;
            let mut schema = Schema::read(&mut block.cursor())?;
            schema.block_index = schema_block_index;
            if schema.name == table {
                self.put_block(schema_block_index, block);
                return Err(eyre!("Table exists"));
            }
            if schema.next_schema_block == 0 {
                let index = self
                    .new_schema_block(table, schema_columns, schema_indices)
                    .await?;
                schema.next_schema_block = index;
                self.mark_schema_dirty(schema.name.clone());
                self.put_schema(schema.name.clone(), Box::new(schema));
                return Ok(Response::Meta { affected_rows: 0 });
            }
            self.put_block(schema_block_index, block);
            let next_schema_block_index = schema.next_schema_block;
            self.put_schema(schema.name.clone(), Box::new(schema));
            schema_block_index = next_schema_block_index;
        }
    }

    pub async fn drop_table(self: &mut Aidb, table: String) -> Result<Response> {
        let mut previous_table = "".to_owned();
        let mut schema_block_index = self.superblock.first_schema_block;
        while schema_block_index != 0 {
            let mut block = self.get_block(schema_block_index).await?;
            let mut schema = Schema::read(&mut block.cursor())?;
            schema.block_index = schema_block_index;
            if schema.name == table {
                if previous_table.is_empty() {
                    // this schema is the first
                    self.superblock.first_schema_block = schema.next_schema_block;
                    self.superblock_dirty = true;
                } else {
                    let mut previous_schema = self.get_schema(&previous_table).await.unwrap();
                    previous_schema.next_schema_block = schema.next_schema_block;
                    self.put_schema(previous_table.clone(), previous_schema);
                    self.mark_schema_dirty(previous_table);
                }
                return Ok(Response::Meta { affected_rows: 0 });
            }
            self.put_block(schema_block_index, block);
            let next_schema_block_index = schema.next_schema_block;
            previous_table = schema.name.clone();
            self.put_schema(schema.name.clone(), Box::new(schema));
            schema_block_index = next_schema_block_index;
        }
        Err(eyre!("table not found"))
    }

    pub(crate) async fn get_schema(self: &mut Aidb, table: &str) -> Result<Box<Schema>> {
        if let Some(schema) = self.schemas.remove(table) {
            return Ok(schema);
        }
        self.load_schema(table).await
    }

    pub(crate) fn put_schema(self: &mut Aidb, table: String, schema: Box<Schema>) {
        self.schemas.insert(table, schema);
    }

    pub(crate) fn mark_schema_dirty(self: &mut Aidb, table: String) {
        self.schemas_dirty.insert(table);
    }

    pub async fn save_schema(&mut self, schema: &Schema) -> Result<()> {
        let mut block = self.get_block(schema.block_index).await?;
        schema.write(&mut block.cursor())?;
        self.put_block(schema.block_index, block);
        self.mark_block_dirty(schema.block_index);
        Ok(())
    }

    pub async fn load_schema(&mut self, table: &str) -> Result<Box<Schema>> {
        let mut schema_block_index = self.superblock.first_schema_block;
        while schema_block_index > 0 {
            let mut block = self.get_block(schema_block_index).await?;
            let mut schema = Schema::read(&mut block.cursor())?;
            schema.block_index = schema_block_index;
            self.put_block(schema_block_index, block);
            if schema.name == table {
                return Ok(Box::new(schema));
            }
            let next_schema_block_index = schema.next_schema_block;
            self.put_schema(schema.name.clone(), Box::new(schema));
            schema_block_index = next_schema_block_index;
        }
        Err(eyre!("table not found"))
    }
}
