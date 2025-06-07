use aidb_core::{Aidb, BlockIoLog, DataType, Response, Row};

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
    QueryOkRows {
        columns: Vec<String>,
        rows: Vec<Row>,
        log: BlockIoLog,
    },
    QueryOkMeta {
        affected_rows: usize,
        log: BlockIoLog,
    },
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
            WorkerRequest::Query(sql) => match aidb.query_log_blocks(sql).await {
                Ok((response, log)) => match response {
                    Response::Rows { columns, rows } => {
                        scope
                            .send(WorkerResponse::QueryOkRows {
                                columns: columns.into_iter().map(|c| c.name).collect(),
                                rows: rows.collect(),
                                log,
                            })
                            .await
                            .unwrap();
                    }
                    Response::Meta { affected_rows } => scope
                        .send(WorkerResponse::QueryOkMeta { affected_rows, log })
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
