use std::{collections::HashMap, iter::repeat};

use crate::{
    Aidb, Column, DataType, Response, Row, RowStream, Value, query,
    sql::{SqlCol, SqlColOrExpr, SqlOn, SqlRel, SqlSelectTarget, SqlWhere},
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
enum PhysicalPlan {
    Projection {
        columns: Vec<ProjectionColumn>,
        inner: Box<PhysicalPlan>,
    },
    CartesianProduct {
        tables: Vec<String>,
    },
    Selection {
        constraints: Vec<SelectionConstraint>,
        inner: Box<PhysicalPlan>,
    },
}

impl Aidb {
    pub(crate) async fn select(
        &mut self,
        columns: Vec<SqlSelectTarget>,
        table: Option<String>,
        join_on: Vec<(String, SqlOn)>,
        where_: Option<SqlWhere>,
        limit: Option<u64>,
    ) -> Result<Response> {
        let (columns, plan) = self
            .build_logical_plan(columns, table, join_on, where_, limit)
            .await?;
        debug!(logical = ?plan);
        // let mut plan = self.build_physical_plan(plan).await?;
        // debug!(physical= ?plan);
        // let mut response = vec![];
        // while let Some(mut rows) = self.execute_select(&mut plan).await? {
        //     response.append(&mut rows);
        // }
        // Ok(Response::Rows {
        //     columns,
        //     rows: RowStream(Box::new(response.into_iter())),
        // })
        Ok(Response::Meta { affected_rows: 42 })
    }

    async fn build_logical_plan(
        &mut self,
        columns: Vec<SqlSelectTarget>,
        table: Option<String>,
        join_on: Vec<(String, SqlOn)>,
        where_: Option<SqlWhere>,
        _limit: Option<u64>, // TODO: limit is not implemented
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
            match self.get_schema(table).await {
                Ok(schema) => {
                    schemas.insert(table.clone(), schema);
                }
                Err(e) => {
                    return Err(e);
                }
            }
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
        };
        for (table, schema) in schemas {
            self.put_schema(table, schema);
        }
        Ok((headers, plan))
    }

    async fn build_physical_plan(&mut self, plan: LogicalQueryPlan) -> Result<PhysicalPlan> {
        let product = Box::new(PhysicalPlan::CartesianProduct {
            tables: plan.tables,
        });
        // Ok(PhysicalPlan::Projection {
        //     columns: plan.columns,
        //     inner: product,
        // })
        todo!()
    }

    async fn execute_select(&mut self, plan: &mut PhysicalPlan) -> Result<Option<Vec<Row>>> {
        match plan {
            PhysicalPlan::Projection { columns, inner } => {
                Box::pin(self.execute_select(inner)).await.map(|r| {
                    r.map(|rows| {
                        rows.into_iter()
                            .map(|row| {
                                columns
                                    .iter()
                                    .map(|column| match column {
                                        ProjectionColumn::Column(index) => row[*index].clone(),
                                        ProjectionColumn::Const(value) => value.clone(),
                                    })
                                    .collect()
                            })
                            .collect()
                    })
                })
            }
            PhysicalPlan::CartesianProduct { tables } => todo!(),
            PhysicalPlan::Selection { constraints, inner } => {
                Box::pin(self.execute_select(inner)).await.map(|r| {
                    r.map(|rows| {
                        rows.into_iter()
                            .filter(|row| {
                                constraints.iter().all(|constraint| match constraint {
                                    SelectionConstraint::EqColumn(lhs, rhs) => {
                                        row[*lhs] == row[*rhs]
                                    }
                                    SelectionConstraint::EqConst(index, value) => {
                                        row[*index] == *value
                                    }
                                })
                            })
                            .collect()
                    })
                })
            }
        }
    }
}
