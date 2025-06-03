use aidb_core::{DataType, Response, Result, Row};

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
    QueryOkColumn(Vec<(String, DataType)>),
    QueryOkRow(Row),
    QueryOkEnd,
    QueryErr(String),
}

#[reactor]
pub async fn Worker(mut scope: ReactorScope<WorkerRequest, WorkerResponse>) {
    while let Some(request) = scope.next().await {
        match request {
            WorkerRequest::Completion(sql) => {
                scope
                    .send(WorkerResponse::Completion("hint".to_owned()))
                    .await
                    .unwrap();
            }
            WorkerRequest::Query(sql) => {
                scope
                    .send(WorkerResponse::QueryErr("error".to_owned()))
                    .await
                    .unwrap();
            }
        }
    }
}

#[allow(unused)]
fn main() {
    console_error_panic_hook::set_once();
    Worker::registrar().register();
}
