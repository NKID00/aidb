use std::io::Cursor;

use binrw::{BinRead, BinWrite, binrw};
use eyre::{OptionExt, Result, eyre};

use crate::{Aidb, BlockIndex, DataType, Response, RowStream, Value};

#[binrw]
#[brw(little)]
#[derive(Debug)]
pub struct Schema {
    next_schema_block: BlockIndex,
    #[br(temp)]
    #[bw(calc = name.len() as u64)]
    name_len: u64,
    #[br(count = name_len, try_map = |s: Vec<u8>| String::from_utf8(s))]
    #[bw(map = |s: &String| s.as_bytes())]
    name: String,
    #[br(temp)]
    #[bw(calc = columns.len() as u64)]
    columns_len: u64,
    #[br(count = columns_len)]
    columns: Vec<Column>,
    row_block: BlockIndex,
    index_block: BlockIndex,
}

#[binrw]
#[brw(little)]
#[derive(Debug, Clone)]
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
            let block = self
                .ensure_block(schema_block_index)
                .await
                .ok_or_eyre("block not found, database corrupted")?;
            let mut cursor = Cursor::new(block.as_slice());
            let schema = Schema::read(&mut cursor)?;
            tables.push(schema.name);
            self.update_cached_block(schema_block_index, block);
            let next_schema_block_index = schema.next_schema_block;
            schema_block_index = next_schema_block_index;
        }
        Ok(Response::Rows {
            columns: vec![Column {
                name: "table_name".to_owned(),
                datatype: DataType::Text,
            }],
            rows: RowStream(Box::new(tables.into_iter().map(|s| vec![Value::Text(s)]))),
        })
    }

    pub async fn describe(self: &mut Aidb, table: String) -> Result<Response> {
        todo!()
    }

    async fn new_schema_block(
        &mut self,
        table: String,
        columns: Vec<Column>,
    ) -> Result<BlockIndex> {
        let (index, mut block) = self
            .new_cached_block()
            .await
            .ok_or_eyre("failed to create block")?;
        let schema = Schema {
            next_schema_block: 0,
            name: table,
            columns,
            row_block: 0,
            index_block: 0,
        };
        let mut cursor = Cursor::new(block.as_mut_slice());
        schema.write(&mut cursor)?;
        self.update_cached_block(index, block);
        self.write_block(index).await?;
        Ok(index)
    }

    pub async fn create_table(
        self: &mut Aidb,
        table: String,
        columns: Vec<Column>,
    ) -> Result<Response> {
        let mut schema_block_index = self.superblock.first_schema_block;
        if schema_block_index == 0 {
            let index = self.new_schema_block(table, columns).await?;
            self.superblock.first_schema_block = index;
            self.write_superblock().await?;
            return Ok(Response::Meta { affected_rows: 0 });
        }
        loop {
            let mut block = self
                .ensure_block(schema_block_index)
                .await
                .ok_or_eyre("block not found, database corrupted")?;
            let mut cursor = Cursor::new(block.as_slice());
            let mut schema = Schema::read(&mut cursor)?;
            if schema.name == table {
                self.update_cached_block(schema_block_index, block);
                return Err(eyre!("Table exists"));
            }
            if schema.next_schema_block == 0 {
                let index = self.new_schema_block(table, columns).await?;
                schema.next_schema_block = index;
                let mut cursor = Cursor::new(block.as_mut_slice());
                schema.write(&mut cursor)?;
                self.update_cached_block(schema_block_index, block);
                self.write_block(schema_block_index).await?;
                return Ok(Response::Meta { affected_rows: 0 });
            }
            self.update_cached_block(schema_block_index, block);
            let next_schema_block_index = schema.next_schema_block;
            schema_block_index = next_schema_block_index;
        }
    }
}
