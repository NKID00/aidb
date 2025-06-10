use aidb_core::{Aidb, BlockIoLog, Response};

use futures::{SinkExt, StreamExt};
use gloo_worker::Registrable;
use gloo_worker::reactor::{ReactorScope, reactor};
use js_sys::global;
use leptos::logging::log;
use serde::{Deserialize, Serialize};
use wasm_bindgen::JsValue;
use web_sys::WorkerGlobalScope;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkerRequest {
    Completion(String),
    Query(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkerResponse {
    Completion(String),
    Query {
        response: Result<(Response, BlockIoLog), String>,
        duration: f64,
    },
}

fn worker_global_scope() -> WorkerGlobalScope {
    Into::<JsValue>::into(global()).into()
}

fn now() -> f64 {
    worker_global_scope().performance().unwrap().now()
}

#[reactor]
pub async fn Worker(mut scope: ReactorScope<WorkerRequest, WorkerResponse>) {
    log!("new database");
    let mut aidb = Aidb::new_memory().await;
    while let Some(request) = scope.next().await {
        match request {
            WorkerRequest::Completion(sql) => {
                let hint = Aidb::complete(sql);
                scope.send(WorkerResponse::Completion(hint)).await.unwrap();
            }
            WorkerRequest::Query(sql) => {
                let time_start = now();
                let response = aidb.query_log_blocks(sql).await;
                let duration = (now() - time_start) / 1000.;
                scope
                    .send(WorkerResponse::Query {
                        response: response.map_err(|e| e.to_string()),
                        duration,
                    })
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
