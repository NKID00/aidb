mod worker;

use crate::worker::{Worker, WorkerRequest, WorkerResponse};

use futures::{SinkExt, StreamExt};
use gloo_worker::Spawnable;
use leptos::{html, logging::log, prelude::*, task::spawn_local};
use wasm_bindgen::prelude::*;

#[derive(Debug, Clone)]
struct Chat {
    id: usize,
    request: String,
    response: String,
}

impl Chat {
    fn new(id: usize, request: String) -> Self {
        Self {
            id,
            request,
            response: "".to_owned(),
        }
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn request(&self) -> &str {
        &self.request
    }

    pub fn response(&self) -> &str {
        &self.response
    }

    fn respond(&mut self, id: usize, response: impl AsRef<str>) {
        self.response += response.as_ref();
        self.id = id;
    }
}

#[derive(Debug, Clone)]
struct ChatHistory {
    chats: Vec<Chat>,
    next_id: usize,
}

impl ChatHistory {
    pub fn new() -> Self {
        Self {
            chats: vec![],
            next_id: 0,
        }
    }

    fn next_id(&mut self) -> usize {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    pub fn submit(&mut self, request: String) {
        let id = self.next_id();
        self.chats.push(Chat::new(id, request));
    }

    pub fn respond(&mut self, response: impl AsRef<str>) {
        let id = self.next_id();
        let Some(chat) = self.chats.last_mut() else {
            panic!("unexpected response");
        };
        chat.respond(id, response);
    }
}

impl From<ChatHistory> for Vec<Chat> {
    fn from(value: ChatHistory) -> Self {
        value.chats
    }
}

#[component]
pub fn App() -> impl IntoView {
    let worker = Worker::spawner().spawn("./worker.js");

    let (chat, set_chat) = signal(ChatHistory::new());
    let (input, set_input) = signal(String::new());
    let (hint, set_hint) = signal("".to_string());
    let input_element = NodeRef::<html::Span>::new();

    Effect::new({
        let worker = worker.fork();
        move |_| {
            let input = input();
            if input.is_empty() {
                set_hint("SQL Input".to_owned());
                return;
            }
            log!("complete: {:?}", input);
            spawn_local({
                let mut worker = worker.fork();
                async move {
                    worker.send(WorkerRequest::Completion(input)).await.unwrap();
                    let Some(WorkerResponse::Completion(hint)) = worker.next().await else {
                        panic!("unexpected response from worker");
                    };
                    set_hint(hint);
                }
            });
        }
    });

    let focus_input = move || {
        let span = input_element.get_untracked().unwrap();
        span.focus().unwrap();
        let selection = window().get_selection().unwrap().unwrap();
        if let Some(text) = span.child_nodes().item(0) {
            let offset = span.text_content().unwrap().chars().count() as u32;
            if offset > 0 {
                log!("len = {}", offset);
                selection
                    .set_position_with_offset(Some(&text), offset)
                    .unwrap();
                return;
            }
        }
        selection.set_position(Some(&span)).unwrap();
    };

    let submit_input = move |input: String| {
        log!("submit: {:?}", input);
        set_chat.update(|chats| chats.submit(input.clone()));
        spawn_local({
            let mut worker = worker.fork();
            async move {
                worker.send(WorkerRequest::Query(input)).await.unwrap();
                let Some(response) = worker.next().await else {
                    panic!("worker exited unexpectedly");
                };
                match response {
                    WorkerResponse::QueryOkColumn(columns) => {
                        set_chat.update(|chat| {
                            chat.respond(format!(
                                "| {} |",
                                columns.into_iter().collect::<Vec<_>>().join(" | ")
                            ))
                        });
                        while let Some(response) = worker.next().await {
                            match response {
                                WorkerResponse::QueryOkRow(row) => set_chat.update(|chat| {
                                    chat.respond(format!(
                                        "| {} |",
                                        row.into_iter()
                                            .map(|value| value.to_string())
                                            .collect::<Vec<_>>()
                                            .join(" | ")
                                    ))
                                }),
                                WorkerResponse::QueryOkEnd => break,
                                WorkerResponse::QueryErr(e) => {
                                    set_chat.update(|chat| chat.respond(e))
                                }
                                _ => panic!("unexpected response from worker"),
                            }
                        }
                    }
                    WorkerResponse::QueryOkMeta { affected_rows } => set_chat.update(|chat| {
                        chat.respond(format!("Query OK, {} rows affected", affected_rows))
                    }),
                    WorkerResponse::QueryErr(e) => set_chat.update(|chat| chat.respond(e)),
                    _ => panic!("unexpected response from worker"),
                }
            }
        });
    };

    view! {
        <div class="flex-1 flex flex-row w-full items-start">
            <div class="w-[40%] h-[100vh] sticky top-0 bg-sky-50 flex justify-center items-center">
                <h2> "OPFS Explorer" </h2>
            </div>
            <div class="min-h-[100vh] flex-1 flex flex-col justify-start items-stretch">
                <div class="px-8 py-4 sticky top-0 z-30 bg-white flex flex-col justify-start items-start">
                    <h2 class="font-bold text-2xl"> "AIDB" </h2>
                    <h3> { env!("CARGO_PKG_VERSION") } </h3>
                </div>
                <div class="p-8 flex-1 z-0 flex flex-col gap-4 justify-start items-stretch [&>div:first-child>hr]:hidden">
                    <For each=move || { Into::<Vec<Chat>>::into(chat().clone()) } key=Chat::id children={ |c| { view! {
                        <div class="flex flex-col justify-start">
                            <hr class="my-8 border-slate-100" />
                            <div class="px-4 py-2 self-end bg-slate-100 rounded-l-xl rounded-br-xl text-wrap break-all ">
                                { c.request().to_owned() }
                            </div>
                            <div class="p-2 self-start text-wrap break-all ">
                                { c.response().to_owned() }
                            </div>
                        </div>
                    } } } />
                </div>
                <div class="min-h-40 sticky bottom-0">
                    <div class="min-h-20 mt-12 mb-8 px-8 w-full flex flex-row items-stretch">
                        <div class="px-4 py-2 z-20 flex-1 border-slate-300 border rounded-xl" on:mousedown=move |ev| {
                            ev.prevent_default();
                            focus_input();
                        }>
                            <span class="h-auto text-wrap break-all outline-none" contenteditable node_ref=input_element on:mousedown=|ev| {
                                ev.stop_propagation();
                            } on:input=move |_| {
                                let span = input_element.get_untracked().unwrap();
                                let mut text = span.text_content().unwrap();
                                if text.contains('\u{feff}') {  // first character typed
                                    text.retain(|c| c != '\u{feff}');
                                    span.set_text_content(Some(&text));
                                    focus_input();
                                }
                                let new_input = text.replace('\u{a0}', " ").trim().to_owned();
                                if input.get_untracked() != new_input {
                                    set_input(new_input);
                                }
                            } on:keydown=move |ev| {
                                if ev.key() == "Enter" {
                                    ev.prevent_default();
                                    let input = input.get_untracked();
                                    if input.is_empty() {
                                        return;
                                    }
                                    let span = input_element.get_untracked().unwrap();
                                    span.set_text_content(Some(""));
                                    set_input("".to_owned());
                                    submit_input(input);
                                }
                            }>
                                "\u{feff}"  // ZERO WIDTH NO-BREAK SPACE to make caret visible
                            </span>
                            <span> "\u{00a0}" </span>
                            <span class="text-gray-400" on:click=move |_| focus_input()> { hint } </span>
                        </div>
                    </div>
                    <div class="w-full h-full absolute bottom-0 z-10 bg-linear-to-b from-white/0 to-white to-30%" />
                </div>
            </div>
        </div>
    }
}

fn main() {
    console_error_panic_hook::set_once();
    let mount_point: web_sys::HtmlElement = document()
        .get_elements_by_tag_name("main")
        .item(0)
        .expect("mount point not found")
        .dyn_into()
        .unwrap();
    mount_point.replace_children_with_node_0();
    mount_to(mount_point, || {
        view! {
            <App/>
        }
    })
    .forget();
}
