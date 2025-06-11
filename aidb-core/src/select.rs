use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
    iter::repeat,
    mem::swap,
    ops::Bound,
};

use crate::{
    Aidb, Column, DataType, Response, Row, Value,
    btree::{BTreeExactState, BTreeRangeState},
    data::DataHeader,
    schema::{IndexInfo, IndexType},
    sql::{SqlCol, SqlColOrExpr, SqlOn, SqlRel, SqlSelectTarget, SqlWhere},
    storage::{BLOCK_SIZE, Block, BlockIndex, BlockOffset},
};

use binrw::BinRead;
use eyre::{OptionExt, Result, eyre};
use itertools::Itertools;
use tracing::debug;

#[derive(Debug)]
enum QueryColumn {
    Column { table: String, column: String },
    Const(Value),
}

#[derive(Debug)]
enum QueryConstraint {
    EqColumn {
        table_lhs: String,
        column_lhs: String,
        table_rhs: String,
        column_rhs: String,
    },
    EqConst {
        table: String,
        column: String,
        value: Value,
    },
}

#[derive(Debug)]
struct LogicalQueryPlan {
    tables: Vec<String>,
    columns: Vec<QueryColumn>,
    constraints: Vec<QueryConstraint>,
    limit: Option<usize>,
}

type ColumnIndex = usize;

#[derive(Debug)]
enum ProjectionColumn {
    Column(ColumnIndex),
    Const(Value),
}

#[derive(Debug)]
enum SelectionConstraint {
    EqColumn(ColumnIndex, ColumnIndex),
    EqConst(ColumnIndex, Value),
}

#[derive(Debug)]
enum ScanState {
    Initialized,
    Running {
        block_index: BlockIndex,
        next_block_index: BlockIndex,
        block: Block,
        offset: BlockOffset,
    },
}

impl Default for ScanState {
    fn default() -> Self {
        Self::Initialized
    }
}

#[derive(Debug)]
struct CartesianProductState {
    first_run: bool,
    previous_row: Vec<Row>,
}

impl Default for CartesianProductState {
    fn default() -> Self {
        Self {
            first_run: true,
            previous_row: vec![],
        }
    }
}

#[derive(Debug)]
enum PhysicalPlan {
    Scan {
        row_size: usize,
        first_block: BlockIndex,
        state: ScanState,
    },
    BTreeExact {
        root: BlockIndex,
        key: i64,
        state: BTreeExactState,
    },
    BTreeRange {
        root: BlockIndex,
        range: (Bound<i64>, Bound<i64>),
        state: BTreeRangeState,
    },
    Projection {
        columns: Vec<ProjectionColumn>,
        inner: Box<PhysicalPlan>,
    },
    CartesianProduct {
        inner: Vec<PhysicalPlan>,
        state: CartesianProductState,
    },
    Selection {
        constraints: Vec<SelectionConstraint>,
        inner: Box<PhysicalPlan>,
    },
    Limit {
        limit: usize,
        inner: Box<PhysicalPlan>,
        state: usize,
    },
}

impl PhysicalPlan {
    fn reset(&mut self, db: &mut Aidb) {
        match self {
            PhysicalPlan::Scan { state, .. } => {
                let mut new_state = Default::default();
                swap(state, &mut new_state);
                let state = new_state;
                if let ScanState::Running {
                    block_index, block, ..
                } = state
                {
                    db.put_block(block_index, block);
                }
            }
            PhysicalPlan::BTreeExact { state, .. } => *state = BTreeExactState::Initialized,
            PhysicalPlan::BTreeRange { state, .. } => *state = BTreeRangeState::Initialized,
            PhysicalPlan::Projection { inner, .. } => inner.reset(db),
            PhysicalPlan::CartesianProduct { inner, state } => {
                for plan in inner {
                    plan.reset(db);
                }
                *state = Default::default();
            }
            PhysicalPlan::Selection { inner, .. } => inner.reset(db),
            PhysicalPlan::Limit { inner, state, .. } => {
                inner.reset(db);
                *state = 0;
            }
        }
    }
}

impl Display for PhysicalPlan {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PhysicalPlan::Scan { first_block, .. } => write!(f, "@{first_block}"),
            PhysicalPlan::BTreeExact { root, key, .. } => write!(f, "btree@{root} = {key}"),
            PhysicalPlan::BTreeRange { root, range, .. } => write!(f, "btree@{root} {range:?}"),
            PhysicalPlan::Projection { columns, inner } => write!(
                f,
                "Π{{{}}} ({inner})",
                columns
                    .iter()
                    .map(|column| match column {
                        ProjectionColumn::Column(index) => format!("${index}"),
                        ProjectionColumn::Const(value) => format!("{value}"),
                    })
                    .collect_vec()
                    .join(", ")
            ),
            PhysicalPlan::CartesianProduct { inner, .. } => {
                if inner.is_empty() {
                    write!(f, "∅")
                } else {
                    write!(
                        f,
                        "{}",
                        inner
                            .iter()
                            .map(|plan| format!("({plan})"))
                            .collect_vec()
                            .join(" × ")
                    )
                }
            }
            PhysicalPlan::Selection { constraints, inner } => write!(
                f,
                "σ{{{}}} ({inner})",
                constraints
                    .iter()
                    .map(|constraint| match constraint {
                        SelectionConstraint::EqColumn(lhs, rhs) => format!("${lhs} = ${rhs}"),
                        SelectionConstraint::EqConst(index, value) => format!("${index} = {value}"),
                    })
                    .collect_vec()
                    .join(" ∧ ")
            ),
            PhysicalPlan::Limit { limit, inner, .. } => write!(f, "limit{{{limit}}} ({inner})"),
        }
    }
}

impl Aidb {
    pub(crate) async fn select(
        &mut self,
        columns: Vec<SqlSelectTarget>,
        table: Option<String>,
        join_on: Vec<(String, SqlOn)>,
        where_: Option<SqlWhere>,
        limit: Option<usize>,
    ) -> Result<Response> {
        let (columns, plan) = self
            .build_logical_plan(columns, table, join_on, where_, limit)
            .await?;
        debug!(logical = ?plan);
        let mut plan = self.build_physical_plan(plan).await?;
        debug!(physical = plan.to_string());
        let mut rows = vec![];
        while let Some(row) = self.execute_select(&mut plan).await? {
            debug!(?row);
            rows.push(row);
        }
        plan.reset(self);
        Ok(Response::Rows { columns, rows })
    }

    pub(crate) async fn explain(
        &mut self,
        columns: Vec<SqlSelectTarget>,
        table: Option<String>,
        join_on: Vec<(String, SqlOn)>,
        where_: Option<SqlWhere>,
        limit: Option<usize>,
    ) -> Result<Response> {
        let (columns, plan) = self
            .build_logical_plan(columns, table, join_on, where_, limit)
            .await?;
        debug!(logical = ?plan);
        let plan = self.build_physical_plan(plan).await?;
        debug!(physical = plan.to_string());
        Ok(Response::Rows {
            columns: vec![Column {
                name: "query_plan".to_owned(),
                datatype: DataType::Text,
            }],
            rows: vec![vec![Value::Text(plan.to_string())]],
        })
    }

    async fn build_logical_plan(
        &mut self,
        columns: Vec<SqlSelectTarget>,
        table: Option<String>,
        join_on: Vec<(String, SqlOn)>,
        where_: Option<SqlWhere>,
        limit: Option<usize>,
    ) -> Result<(Vec<Column>, LogicalQueryPlan)> {
        // if selects const value only
        if columns.iter().all(|column| match column {
            SqlSelectTarget::Column(_) | SqlSelectTarget::Wildcard => false,
            SqlSelectTarget::Const(_) | SqlSelectTarget::Variable(_) => true,
        }) {
            if (!join_on.is_empty()) || (where_.is_some()) {
                return Err(eyre!("table required"));
            }
            let (headers, columns) = columns
                .into_iter()
                .map(|column| {
                    let name = column.to_string();
                    match column {
                        SqlSelectTarget::Const(v) => (
                            Column {
                                name,
                                datatype: v.datatype().unwrap_or(DataType::Text),
                            },
                            QueryColumn::Const(v),
                        ),
                        SqlSelectTarget::Variable(v) => (
                            Column {
                                name,
                                datatype: DataType::Text,
                            },
                            QueryColumn::Const(match v.as_str() {
                                "@@version_comment" => Value::Text("aidb".to_owned()),
                                _ => Value::Null,
                            }),
                        ),
                        _ => unreachable!(),
                    }
                })
                .unzip();
            return Ok((
                headers,
                LogicalQueryPlan {
                    tables: vec![],
                    columns,
                    constraints: vec![],
                    limit,
                },
            ));
        }

        let from_table = table.ok_or_eyre("table required")?;
        let mut headers = vec![];
        let mut tables = vec![from_table.clone()];
        tables.extend(join_on.iter().map(|(table, _on)| table.clone()));
        let mut query_columns = vec![];
        let mut constraints = vec![];

        let mut schemas = HashMap::new();
        for table in tables.iter() {
            if schemas.contains_key(table) {
                Err(eyre!("duplicate table"))?;
            }
            let schema = self.get_schema(table).await?;
            schemas.insert(table.clone(), schema);
        }

        let reify_column = |column| -> Result<(String, String, DataType)> {
            match column {
                SqlCol::Full { table, column } => {
                    let Some(schema) = schemas.get(&table) else {
                        return Err(eyre!("table not specified"));
                    };
                    let Some(Column { datatype, .. }) =
                        schema.columns.iter().find(|c| column == c.name)
                    else {
                        return Err(eyre!("column not found"));
                    };
                    Ok((table, column, *datatype))
                }
                SqlCol::Short(column) => {
                    let matched_columns = schemas
                        .iter()
                        .flat_map(|(table, schema)| repeat(table).zip(schema.columns.iter()))
                        .filter(|(_, c)| column == c.name)
                        .map(|(t, c)| (t.clone(), c.clone()))
                        .collect_vec();
                    if matched_columns.is_empty() {
                        Err(eyre!("column not found"))?
                    } else if matched_columns.len() > 1 {
                        Err(eyre!("ambiguous column"))?;
                    }
                    let (table, Column { datatype, .. }) =
                        matched_columns.into_iter().next().unwrap();
                    Ok((table, column, datatype))
                }
            }
        };

        for column in columns {
            let name = column.to_string();
            match column {
                SqlSelectTarget::Column(column) => {
                    let (table, column, datatype) = reify_column(column)?;
                    headers.push(Column { name, datatype });
                    query_columns.push(QueryColumn::Column { table, column });
                }
                SqlSelectTarget::Wildcard => {
                    let schema = schemas.get(&from_table).unwrap();
                    headers.extend(schema.columns.iter().cloned());
                    query_columns.extend(schema.columns.iter().map(|column| QueryColumn::Column {
                        table: from_table.clone(),
                        column: column.name.clone(),
                    }));
                }
                SqlSelectTarget::Const(v) => {
                    headers.push(Column {
                        name,
                        datatype: v.datatype().unwrap_or(DataType::Text),
                    });
                    query_columns.push(QueryColumn::Const(v));
                }
                SqlSelectTarget::Variable(v) => {
                    headers.push(Column {
                        name,
                        datatype: DataType::Text,
                    });
                    query_columns.push(QueryColumn::Const(match v.as_str() {
                        "@@version_comment" => Value::Text("aidb".to_owned()),
                        _ => Value::Null,
                    }));
                }
            }
        }

        for (_, on) in join_on {
            let (table_lhs, column_lhs, datatype_lhs) = reify_column(on.lhs)?;
            let (table_rhs, column_rhs, datatype_rhs) = reify_column(on.rhs)?;
            if datatype_lhs != datatype_rhs {
                Err(eyre!("datatype mismatch"))?;
            }
            constraints.push(QueryConstraint::EqColumn {
                table_lhs,
                column_lhs,
                table_rhs,
                column_rhs,
            });
        }

        fn reify_where(
            reify_column: &impl Fn(SqlCol) -> Result<(String, String, DataType)>,
            where_: SqlWhere,
        ) -> Result<Vec<QueryConstraint>> {
            match where_ {
                SqlWhere::Rel(SqlRel::Eq {
                    lhs: SqlColOrExpr::Column(lhs),
                    rhs: SqlColOrExpr::Column(rhs),
                }) => {
                    let (table_lhs, column_lhs, datatype_lhs) = reify_column(lhs)?;
                    let (table_rhs, column_rhs, datatype_rhs) = reify_column(rhs)?;
                    if datatype_lhs != datatype_rhs {
                        Err(eyre!("datatype mismatch"))?;
                    }
                    Ok(vec![QueryConstraint::EqColumn {
                        table_lhs,
                        column_lhs,
                        table_rhs,
                        column_rhs,
                    }])
                }
                SqlWhere::Rel(SqlRel::Eq {
                    lhs: SqlColOrExpr::Const(value),
                    rhs: SqlColOrExpr::Column(column),
                })
                | SqlWhere::Rel(SqlRel::Eq {
                    lhs: SqlColOrExpr::Column(column),
                    rhs: SqlColOrExpr::Const(value),
                }) => {
                    let (table, column, datatype) = reify_column(column)?;
                    if let Some(value_datatype) = value.datatype()
                        && datatype != value_datatype
                    {
                        Err(eyre!("datatype mismatch"))?;
                    }
                    Ok(vec![QueryConstraint::EqConst {
                        table,
                        column,
                        value,
                    }])
                }
                SqlWhere::Rel(SqlRel::Eq {
                    lhs: SqlColOrExpr::Const(lhs),
                    rhs: SqlColOrExpr::Const(rhs),
                }) => {
                    if lhs == rhs {
                        Ok(vec![])
                    } else {
                        Err(eyre!("where clause is always false"))
                    }
                }
                SqlWhere::Rel(SqlRel::Le { .. }) => todo!(),
                SqlWhere::Rel(SqlRel::Like { .. }) => todo!(),
                SqlWhere::And(lhs, rhs) => {
                    let mut constraints = reify_where(reify_column, *lhs)?;
                    constraints.append(&mut reify_where(reify_column, *rhs)?);
                    Ok(constraints)
                }
                SqlWhere::Or(_lhs, _rhs) => todo!(),
                SqlWhere::Not(_clause) => todo!(),
            }
        }

        if let Some(where_) = where_ {
            constraints.append(&mut reify_where(&reify_column, where_)?);
        }

        let plan = LogicalQueryPlan {
            tables,
            columns: query_columns,
            constraints,
            limit,
        };
        for (table, schema) in schemas {
            self.put_schema(table, schema);
        }
        Ok((headers, plan))
    }

    async fn build_physical_plan(&mut self, mut logical: LogicalQueryPlan) -> Result<PhysicalPlan> {
        let mut columns = vec![];
        let mut row_sizes = HashMap::new();
        let mut first_blocks = HashMap::new();
        for table in logical.tables.iter() {
            let schema = self.get_schema(table).await?;
            row_sizes.insert(table.clone(), schema.row_size());
            first_blocks.insert(table.clone(), schema.data_block);
            for (i, column) in schema.columns.iter().enumerate() {
                columns.push((
                    table.clone(),
                    column.name.clone(),
                    schema
                        .indices
                        .iter()
                        .find(|IndexInfo { column_index, .. }| i == *column_index as usize)
                        .map(|IndexInfo { type_, block, .. }| (*type_, *block)),
                ));
            }
            self.put_schema(table.clone(), schema);
        }
        let find_column_index = |table: &str, column: &str| -> ColumnIndex {
            columns
                .iter()
                .position(|(t, c, _)| t == table && c == column)
                .unwrap()
        };
        let find_column_index_info =
            |table: &str, column: &str| -> Option<(IndexType, BlockIndex)> {
                columns
                    .iter()
                    .enumerate()
                    .find(|(_, (t, c, _))| t == table && c == column)
                    .unwrap()
                    .1
                    .2
            };

        let mut plans = vec![];
        for table in logical.tables.iter() {
            let mut indexed = false;
            let mut constraints_remaining = vec![];
            for constraint in logical.constraints.into_iter() {
                if let QueryConstraint::EqConst {
                    table,
                    column,
                    value,
                } = &constraint
                    && let Some((type_, block)) = find_column_index_info(table, column)
                {
                    match type_ {
                        IndexType::BTree => {
                            let key = match value.clone() {
                                Value::Integer(key) => key,
                                Value::Null => {
                                    return Err(eyre!("indexed column must not be NULL"));
                                }
                                _ => return Err(eyre!("datatype mismatch")),
                            };
                            plans.push(PhysicalPlan::BTreeExact {
                                root: block,
                                key,
                                state: Default::default(),
                            });
                            indexed = true;
                            continue;
                        }
                    }
                }
                constraints_remaining.push(constraint);
            }
            logical.constraints = constraints_remaining;
            if !indexed {
                plans.push(PhysicalPlan::Scan {
                    row_size: *row_sizes.get(table).unwrap(),
                    first_block: *first_blocks.get(table).unwrap(),
                    state: Default::default(),
                })
            }
        }

        let plan = if plans.len() == 1 {
            plans.pop().unwrap()
        } else {
            PhysicalPlan::CartesianProduct {
                inner: plans,
                state: Default::default(),
            }
        };

        let plan = if logical.constraints.is_empty() {
            plan
        } else {
            PhysicalPlan::Selection {
                constraints: logical
                    .constraints
                    .into_iter()
                    .map(|constraint| match constraint {
                        QueryConstraint::EqColumn {
                            table_lhs,
                            column_lhs,
                            table_rhs,
                            column_rhs,
                        } => SelectionConstraint::EqColumn(
                            find_column_index(&table_lhs, &column_lhs),
                            find_column_index(&table_rhs, &column_rhs),
                        ),
                        QueryConstraint::EqConst {
                            table,
                            column,
                            value,
                        } => {
                            SelectionConstraint::EqConst(find_column_index(&table, &column), value)
                        }
                    })
                    .collect(),
                inner: Box::new(plan),
            }
        };

        let plan = PhysicalPlan::Projection {
            columns: logical
                .columns
                .into_iter()
                .map(|column| match column {
                    QueryColumn::Column { table, column } => {
                        ProjectionColumn::Column(find_column_index(&table, &column))
                    }
                    QueryColumn::Const(value) => ProjectionColumn::Const(value),
                })
                .collect(),
            inner: Box::new(plan),
        };

        let plan = match logical.limit {
            Some(limit) => PhysicalPlan::Limit {
                limit,
                inner: Box::new(plan),
                state: Default::default(),
            },
            None => plan,
        };

        Ok(plan)
    }

    async fn execute_select(&mut self, plan: &mut PhysicalPlan) -> Result<Option<Row>> {
        debug!(plan = plan.to_string());
        match plan {
            PhysicalPlan::Scan {
                row_size,
                first_block,
                state,
            } => match state {
                ScanState::Initialized => {
                    debug!(first_block);
                    if *first_block == 0 {
                        Ok(None)
                    } else {
                        let mut block = self.get_block(*first_block).await?;
                        let mut cursor = block.cursor();
                        let header = DataHeader::read(&mut cursor)?;
                        let offset = cursor.position() as BlockOffset;
                        *state = ScanState::Running {
                            block_index: *first_block,
                            next_block_index: header.next_data_block,
                            block,
                            offset,
                        };
                        Box::pin(self.execute_select(plan)).await
                    }
                }
                ScanState::Running {
                    next_block_index,
                    block,
                    offset,
                    ..
                } => {
                    debug!(next_block_index);
                    let mut cursor = block.cursor_at(*offset);
                    while (BLOCK_SIZE as isize - cursor.position() as isize) > *row_size as isize {
                        let position = cursor.position();
                        if let Some(row) = self.read_row(&mut cursor).await? {
                            *offset = cursor.position() as u16;
                            return Ok(Some(row));
                        };
                        cursor.set_position(position + *row_size as u64);
                    }
                    if *next_block_index == 0 {
                        let mut new_state = ScanState::Initialized;
                        swap(state, &mut new_state);
                        let ScanState::Running {
                            block_index, block, ..
                        } = new_state
                        else {
                            unreachable!()
                        };
                        self.put_block(block_index, block);
                        Ok(None)
                    } else {
                        let mut block = self.get_block(*next_block_index).await?;
                        let mut cursor = block.cursor();
                        let header = DataHeader::read(&mut cursor)?;
                        let offset = cursor.position() as BlockOffset;
                        let mut new_state = ScanState::Running {
                            block_index: *next_block_index,
                            next_block_index: header.next_data_block,
                            block,
                            offset,
                        };
                        swap(state, &mut new_state);
                        let ScanState::Running {
                            block_index, block, ..
                        } = new_state
                        else {
                            unreachable!()
                        };
                        self.put_block(block_index, block);
                        Box::pin(self.execute_select(plan)).await
                    }
                }
            },
            PhysicalPlan::BTreeExact { root, key, state } => {
                let Some(ptr) = self.select_btree(*root, *key, state).await? else {
                    return Ok(None);
                };
                let mut block = self.get_block(ptr.block).await?;
                let mut cursor = block.cursor_at(ptr.offset);
                let row = self.read_row(&mut cursor).await?;
                self.put_block(ptr.block, block);
                Ok(row)
            }
            PhysicalPlan::BTreeRange { root, range, state } => {
                let Some(ptr) = self.select_range_btree(*root, *range, state).await? else {
                    return Ok(None);
                };
                let mut block = self.get_block(ptr.block).await?;
                let mut cursor = block.cursor_at(ptr.offset);
                let row = self.read_row(&mut cursor).await?;
                self.put_block(ptr.block, block);
                Ok(row)
            }
            PhysicalPlan::Projection { columns, inner } => {
                let Some(row) = Box::pin(self.execute_select(inner)).await? else {
                    return Ok(None);
                };
                let row = columns
                    .iter()
                    .map(|column| match column {
                        ProjectionColumn::Column(index) => row[*index].clone(),
                        ProjectionColumn::Const(value) => value.clone(),
                    })
                    .collect();
                Ok(Some(row))
            }
            PhysicalPlan::CartesianProduct { inner, state } => {
                if inner.is_empty() {
                    if state.first_run {
                        state.first_run = false;
                        return Ok(Some(vec![]));
                    } else {
                        return Ok(None);
                    }
                }
                if state.first_run {
                    state.first_run = false;
                    for plan in inner.iter_mut() {
                        match Box::pin(self.execute_select(plan)).await? {
                            Some(row) => {
                                state.previous_row.push(row);
                            }
                            None => {
                                return Ok(None);
                            }
                        }
                    }
                    Ok(Some(state.previous_row.iter().flatten().cloned().collect()))
                } else {
                    let mut index = 0;
                    loop {
                        match Box::pin(self.execute_select(&mut inner[index])).await? {
                            Some(row) => {
                                state.previous_row[index] = row;
                                return Ok(Some(
                                    state.previous_row.iter().flatten().cloned().collect(),
                                ));
                            }
                            None => {
                                inner[index].reset(self);
                                index += 1;
                                if index >= inner.len() {
                                    return Ok(None);
                                }
                            }
                        }
                    }
                }
            }
            PhysicalPlan::Selection { constraints, inner } => {
                while let Some(row) = Box::pin(self.execute_select(inner)).await? {
                    if constraints.iter().all(|constraint| match constraint {
                        SelectionConstraint::EqColumn(lhs, rhs) => row[*lhs] == row[*rhs],
                        SelectionConstraint::EqConst(index, value) => row[*index] == *value,
                    }) {
                        return Ok(Some(row));
                    }
                }
                Ok(None)
            }
            PhysicalPlan::Limit {
                limit,
                inner,
                state,
            } => {
                if state < limit {
                    *state += 1;
                    Box::pin(self.execute_select(inner)).await
                } else {
                    Ok(None)
                }
            }
        }
    }
}
