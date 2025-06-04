use aidb_core::{Aidb, DataType, Response, Row};

use futures::{SinkExt, StreamExt};
use gloo_worker::Registrable;
use gloo_worker::reactor::{ReactorScope, reactor};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkerRequest {
    Completion(String),
    Query(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkerResponse {
    Completion(String),
    QueryOkColumn(Vec<String>),
    QueryOkRow(Row),
    QueryOkEnd,
    QueryOkMeta { affected_rows: usize },
    QueryErr(String),
}

#[reactor]
pub async fn Worker(mut scope: ReactorScope<WorkerRequest, WorkerResponse>) {
    let mut aidb = Aidb::new_memory().await;
    while let Some(request) = scope.next().await {
        match request {
            WorkerRequest::Completion(sql) => {
                let hint = Aidb::complete(sql);
                scope.send(WorkerResponse::Completion(hint)).await.unwrap();
            }
            WorkerRequest::Query(sql) => match aidb.query(sql).await {
                Ok(response) => match response {
                    Response::Rows { columns, rows } => {
                        scope
                            .send(WorkerResponse::QueryOkColumn(
                                columns.into_iter().map(|c| c.name).collect(),
                            ))
                            .await
                            .unwrap();
                        for row in rows {
                            scope.send(WorkerResponse::QueryOkRow(row)).await.unwrap();
                        }
                        scope.send(WorkerResponse::QueryOkEnd).await.unwrap();
                    }
                    Response::Meta { affected_rows } => scope
                        .send(WorkerResponse::QueryOkMeta { affected_rows })
                        .await
                        .unwrap(),
                },
                Err(e) => scope
                    .send(WorkerResponse::QueryErr(e.to_string()))
                    .await
                    .unwrap(),
            },
        }
    }
}

#[allow(unused)]
fn main() {
    console_error_panic_hook::set_once();
    Worker::registrar().register();
}
