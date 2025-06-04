use std::io::Cursor;

use binrw::{BinRead, binrw};
use eyre::{OptionExt, Result};

use crate::{Aidb, BlockIndex, DataType, Response, RowStream, Value, sql::SqlColDef};

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
}

#[binrw]
#[brw(little)]
#[derive(Debug)]
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
        let mut schema_block = self.superblock.first_schema_block;
        let mut tables = vec!["a".to_owned()];
        while schema_block > 0 {
            let block = self
                .ensure_block(schema_block)
                .await
                .ok_or_eyre("block not found, databas corrupted")?;
            let mut cursor = Cursor::new(block.as_slice());
            let schema = Schema::read(&mut cursor)?;
            tables.push(schema.name);
            schema_block = schema.next_schema_block;
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

    pub async fn create_table(
        self: &mut Aidb,
        table: String,
        columns: Vec<SqlColDef>,
    ) -> Result<Response> {
        todo!()
    }
}
