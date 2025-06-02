use aidb_core::{DataType, Response, Result, Row};

use futures::{SinkExt, StreamExt};
use gloo_worker::Spawnable;
use gloo_worker::reactor::{ReactorScope, reactor};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
enum WorkerRequest {
    Completion(String),
    Query(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum WorkerResponse {
    Completion(String),
    QueryOkColumn(Vec<(String, DataType)>),
    QueryOkRow(Row),
    QueryOkEnd,
    QueryErr(String),
}

#[reactor]
async fn Worker(mut scope: ReactorScope<WorkerRequest, WorkerResponse>) {
    // while let Some(m) = scope.next().await {
    //     if scope.send(m.pow(2)).await.is_err() {
    //         break;
    //     }
    // }
}
