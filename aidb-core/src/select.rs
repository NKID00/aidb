use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
    iter::repeat,
};

use crate::{
    Aidb, Column, DataType, Response, Row, Value,
    sql::{SqlCol, SqlColOrExpr, SqlOn, SqlRel, SqlSelectTarget, SqlWhere},
    storage::DataPointer,
};

use eyre::{OptionExt, Result, eyre};
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
struct ScanState {
    next_record: DataPointer,
}

impl Default for ScanState {
    fn default() -> Self {
        Self {
            next_record: DataPointer {
                block: 0,
                offset: 0,
            },
        }
    }
}

#[derive(Debug, Default)]
struct BTreeState {}

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
        table: String,
        state: ScanState,
    },
    BTree {
        table: String,
        key: i64,
        state: BTreeState,
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
    fn restart(&mut self) {
        match self {
            PhysicalPlan::Scan { state, .. } => *state = Default::default(),
            PhysicalPlan::BTree { state, .. } => *state = Default::default(),
            PhysicalPlan::Projection { inner, .. } => inner.restart(),
            PhysicalPlan::CartesianProduct { inner, state } => {
                for plan in inner {
                    plan.restart();
                }
                *state = Default::default();
            }
            PhysicalPlan::Selection { inner, .. } => inner.restart(),
            PhysicalPlan::Limit { inner, state, .. } => {
                inner.restart();
                *state = 0;
            }
        }
    }
}

impl Display for PhysicalPlan {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PhysicalPlan::Scan { table, .. } => write!(f, "{table}"),
            PhysicalPlan::BTree { table, key, state } => write!(f, "btree{table}"),
            PhysicalPlan::Projection { columns, inner } => write!(
                f,
                "Π{{{}}} ({inner})",
                columns
                    .iter()
                    .map(|column| match column {
                        ProjectionColumn::Column(index) => format!("${index}"),
                        ProjectionColumn::Const(value) => format!("{value}"),
                    })
                    .collect::<Vec<_>>()
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
                            .collect::<Vec<_>>()
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
                    .collect::<Vec<_>>()
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
        debug!("physical = {plan}");
        let mut rows = vec![];
        while let Some(row) = self.execute_select(&mut plan).await? {
            rows.push(row);
        }
        Ok(Response::Rows { columns, rows })
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
                    let matched_columns: Vec<_> = schemas
                        .iter()
                        .flat_map(|(table, schema)| repeat(table).zip(schema.columns.iter()))
                        .filter(|(_, c)| column == c.name)
                        .map(|(t, c)| (t.clone(), c.clone()))
                        .collect();
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

    async fn build_physical_plan(&mut self, plan: LogicalQueryPlan) -> Result<PhysicalPlan> {
        let product = Box::new(PhysicalPlan::CartesianProduct {
            inner: plan
                .tables
                .iter()
                .map(|table| PhysicalPlan::Scan {
                    table: table.clone(),
                    state: Default::default(),
                })
                .collect(),
            state: Default::default(),
        });
        let mut columns = vec![];
        for table in plan.tables.into_iter() {
            let schema = self.get_schema(&table).await?;
            columns.extend(
                schema
                    .columns
                    .iter()
                    .map(|column| (table.clone(), column.name.clone())),
            );
            self.put_schema(table, schema);
        }
        let find_column_index = |table: &str, column: &str| -> ColumnIndex {
            columns
                .iter()
                .enumerate()
                .find(|(_, (t, c))| t == table && c == column)
                .unwrap()
                .0
        };

        let selection = PhysicalPlan::Selection {
            constraints: plan
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
                    } => SelectionConstraint::EqConst(find_column_index(&table, &column), value),
                })
                .collect(),
            inner: product,
        };

        let projection = PhysicalPlan::Projection {
            columns: plan
                .columns
                .into_iter()
                .map(|column| match column {
                    QueryColumn::Column { table, column } => {
                        ProjectionColumn::Column(find_column_index(&table, &column))
                    }
                    QueryColumn::Const(value) => ProjectionColumn::Const(value),
                })
                .collect(),
            inner: Box::new(selection),
        };

        let plan = match plan.limit {
            Some(limit) => PhysicalPlan::Limit {
                limit,
                inner: Box::new(projection),
                state: Default::default(),
            },
            None => projection,
        };

        Ok(plan)
    }

    async fn execute_select(&mut self, plan: &mut PhysicalPlan) -> Result<Option<Row>> {
        match plan {
            PhysicalPlan::Scan { table, state } => {
                todo!()
            }
            PhysicalPlan::BTree { table, key, state } => todo!(),
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
                                if index >= inner.len() {
                                    return Ok(None);
                                } else {
                                    inner[index].restart();
                                    index += 1;
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
                if *state < *limit {
                    *state += 1;
                    Box::pin(self.execute_select(inner)).await
                } else {
                    Ok(None)
                }
            }
        }
    }
}
