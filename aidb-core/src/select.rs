use crate::{
    Aidb, Column, DataType, Response, RowStream, Value,
    sql::{SqlOn, SqlSelectTarget, SqlWhere},
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
    columns: Vec<(String, DataType, QueryColumn)>,
    constraints: Vec<QueryConstraint>,
}

#[derive(Debug)]
enum PhysicalPlan {
    None,
    Scan(String),
    Projection {
        columns: Vec<QueryColumn>,
        inner: Box<PhysicalPlan>,
    },
    CartesianProduct(Vec<PhysicalPlan>),
    Selection {
        constraints: Vec<QueryConstraint>,
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
        let plan = self.build_logical_plan(columns, table, join_on, where_, limit)?;
        debug!(logical = ?plan);
        let plan = self.build_physical_plan(plan)?;
        debug!(physical= ?plan);
        self.execute(plan)
    }

    fn build_logical_plan(
        &mut self,
        columns: Vec<SqlSelectTarget>,
        table: Option<String>,
        join_on: Vec<(String, SqlOn)>,
        where_: Option<SqlWhere>,
        _limit: Option<u64>, // TODO: limit is not implemented
    ) -> Result<(Vec<(String, DataType)>, LogicalQueryPlan)> {
        // if selects const value only
        if columns.iter().all(|column| match column {
            SqlSelectTarget::Column(_) | SqlSelectTarget::Wildcard => false,
            SqlSelectTarget::Const(_) | SqlSelectTarget::Variable(_) => true,
        }) {
            if (!join_on.is_empty()) || (where_.is_some()) {
                return Err(eyre!("table required"));
            }
            return Ok(LogicalQueryPlan {
                tables: vec![],
                
                columns: columns
                    .into_iter()
                    .map(|column| match column {
                        SqlSelectTarget::Const(v) => (
                            v.to_string(),
                            v.datatype().unwrap_or(DataType::Text),
                            QueryColumn::Const(v),
                        ),
                        SqlSelectTarget::Variable(v) => (
                            v.clone(),
                            DataType::Text,
                            QueryColumn::Const(match v.as_str() {
                                "@@version_comment" => Value::Text("aidb".to_owned()),
                                _ => Value::Null,
                            }),
                        ),
                        _ => unreachable!(),
                    })
                    .collect(),
                constraints: vec![],
            });
        }
        let table = table.ok_or_eyre("table required")?;
        let tables = vec![table];
        let query_columns = vec![];
        let constraints = vec![];
        let logical_plan = LogicalQueryPlan {
            tables,
            columns: query_columns,
            constraints,
        };
        // let schema = self.get_schema(&table).await?;

        // self.put_schema(table, schema);
        todo!()
    }

    fn build_physical_plan(&mut self, plan: LogicalQueryPlan) -> Result<PhysicalPlan> {
        let mut product = vec![];
        product.extend(
            plan.tables
                .into_iter()
                .map(|table| PhysicalPlan::Scan(table)),
        );
        let mut temp_counter = 0;
        product.extend(plan.columns.iter().filter_map(|column| match column {
            QueryColumn::Column { table, column } => {}
            QueryColumn::Const(v) => 
        }));
    }

    fn execute(&mut self, plan: PhysicalPlan) -> Result<Response> {
        //     let mut response_columns = Vec::new();
        //     let mut row = Vec::new();
        //     for column in columns {
        //         match column {
        //             SqlSelectTarget::Const(v) => {
        //                 response_columns.push(Column {
        //                     name: v.to_string(),
        //                     datatype: v.datatype().unwrap_or(DataType::Integer),
        //                 });
        //                 row.push(v);
        //             }
        //             SqlSelectTarget::Variable(v) => {
        //                 response_columns.push(Column {
        //                     name: v.clone(),
        //                     datatype: DataType::Text,
        //                 });
        //                 row.push(match v.as_str() {
        //                     "@@version_comment" => Value::Text("aidb".to_owned()),
        //                     _ => Value::Null,
        //                 });
        //             }
        //             _ => unreachable!(),
        //         }
        //     }
        //     return Ok(Response::Rows {
        //         columns: response_columns,
        //         rows: RowStream(Box::new(vec![row].into_iter())),
        //     });
        todo!()
    }
}
