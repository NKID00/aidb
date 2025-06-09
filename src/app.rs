mod worker;

use std::{
    collections::BTreeMap,
    hash::{DefaultHasher, Hash, Hasher},
    rc::Rc,
};

use crate::worker::{Worker, WorkerRequest, WorkerResponse};

use aidb_core::BlockIoLog;
use futures::{SinkExt, StreamExt, lock::Mutex};
use gloo_worker::Spawnable;
use leptos::{html, logging::log, prelude::*, task::spawn_local};
use wasm_bindgen::prelude::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum BlockStatus {
    Normal,
    Read,
    Written,
}

#[derive(Debug, Clone)]
struct BlockList {
    blocks: BTreeMap<u64, BlockStatus>,
}

impl BlockList {
    fn new() -> Self {
        Self {
            blocks: (0..200).map(|i| (i, BlockStatus::Normal)).collect(),
        }
    }

    fn update(&mut self, log: BlockIoLog) {
        use BlockStatus::*;
        for (_, status) in self.blocks.iter_mut() {
            *status = Normal;
        }
        for b in log.read {
            self.blocks.insert(b, Read);
        }
        for b in log.written {
            self.blocks.insert(b, Written);
        }
    }
}

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

#[component]
pub fn App() -> impl IntoView {
    let worker = Rc::new(Mutex::new(Worker::spawner().spawn("./worker.js")));

    let (blocks, set_blocks) = signal(BlockList::new());
    let (chat, set_chat) = signal(ChatHistory::new());
    let (input, set_input) = signal(String::new());
    let (hint, set_hint) = signal("".to_string());
    let input_ref = NodeRef::<html::Code>::new();

    Effect::new({
        let worker = worker.clone();
        move |_| {
            let input = input();
            if input.is_empty() {
                set_hint("SQL Input".to_owned());
                return;
            }
            log!("complete: {:?}", input);
            spawn_local({
                let worker = worker.clone();
                async move {
                    let mut worker = worker.lock().await;
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
        let input_element = input_ref.get_untracked().unwrap();
        input_element.focus().unwrap();
        let selection = window().get_selection().unwrap().unwrap();
        if let Some(text) = input_element.child_nodes().item(0) {
            let offset = input_element.text_content().unwrap().chars().count() as u32;
            if offset > 0 {
                selection
                    .set_position_with_offset(Some(&text), offset)
                    .unwrap();
                return;
            }
        }
        selection.set_position(Some(&input_element)).unwrap();
    };

    let update_input = move |text: String| {
        let new_input = text.replace('\u{a0}', " ").trim().to_owned();
        if input.get_untracked() != new_input {
            set_input(new_input);
        }
    };

    let paste_input = move |input: String| {
        let input_element = input_ref.get_untracked().unwrap();
        let selection = window().get_selection().unwrap().unwrap();
        if !selection
            .contains_node_with_allow_partial_containment(&input_element, true)
            .unwrap()
        {
            return;
        }
        let Some(text_node) = input_element.child_nodes().item(0) else {
            return;
        };
        let mut text = input_element.text_content().unwrap();
        let Ok(range) = selection.get_range_at(0) else {
            return;
        };
        let start_offset = range.start_offset().unwrap() as usize;
        if range.collapsed() {
            if let Some((offset, _)) = text.char_indices().nth(start_offset) {
                text.insert_str(offset, &input);
            } else {
                text.push_str(&input);
            };
        } else {
            let end_offset = range.end_offset().unwrap() as usize;
            text.replace_range(start_offset..end_offset, &input);
        }
        text_node.set_text_content(Some(&text));
        selection
            .set_position_with_offset(
                Some(&text_node),
                (start_offset + input.chars().count()) as u32,
            )
            .unwrap();
        update_input(text);
    };

    let submit_input = move |input: String| {
        log!("submit: {:?}", input);
        set_chat.update(|chats| chats.submit(input.clone()));
        spawn_local({
            let worker = worker.clone();
            async move {
                let mut worker = worker.lock().await;
                worker.send(WorkerRequest::Query(input)).await.unwrap();
                let Some(response) = worker.next().await else {
                    panic!("worker exited unexpectedly");
                };
                match response {
                    WorkerResponse::QueryOkRows { columns, rows, log } => {
                        let len = rows.len();
                        if len == 0 {
                            set_chat.update(|chat| chat.respond("Empty set"));
                        } else {
                            set_chat.update(|chat| {
                                chat.respond(
                                    format!(
                                        "| {} |\n",
                                        columns.into_iter().collect::<Vec<_>>().join(" | ")
                                    ) + &rows
                                        .into_iter()
                                        .map(|row| {
                                            format!(
                                                "| {} |\n",
                                                row.into_iter()
                                                    .map(|value| value.to_string())
                                                    .collect::<Vec<_>>()
                                                    .join(" | ")
                                            )
                                        })
                                        .collect::<String>()
                                        + &format!("{len} rows in set"),
                                )
                            });
                        }
                        set_blocks.update(|bl| bl.update(log));
                    }
                    WorkerResponse::QueryOkMeta { affected_rows, log } => set_chat.update(|chat| {
                        chat.respond(format!("Query OK, {affected_rows} rows affected"));
                        set_blocks.update(|bl| bl.update(log));
                    }),
                    WorkerResponse::QueryErr(e) => {
                        set_chat.update(|chat| chat.respond(format!("ERROR: {e}")));
                        set_blocks.update(|bl| bl.update(BlockIoLog::default()));
                    }
                    _ => panic!("unexpected response from worker"),
                }
            }
        });
    };

    view! {
        <div class="flex-1 flex flex-row w-full items-start divide-solid divide-x-1 divide-slate-300">
            <div class="w-[25%] h-[100vh] sticky top-0 flex flex-col justify-start items-center">
                <h2 class="m-4 text-lg"> "Blocks" </h2>
                <div class="z-0 grid grid-cols-8 gap-2 justify-start justify-items-center content-start place-content-center overflow-hidden">
                    <For each=move || { blocks().blocks.clone() } key=|f| {
                        let mut hasher = DefaultHasher::new();
                        f.hash(&mut hasher);
                        hasher.finish()
                    } children={ |(name, status)| { view! {
                        <div class={ "w-10 h-10 flex justify-center items-center rounded ".to_owned() + match status {
                            BlockStatus::Normal => "bg-slate-50",
                            BlockStatus::Read => "bg-sky-100",
                            BlockStatus::Written => "bg-orange-100",
                        } }> <code> { name } </code> </div>
                    } } } />
                </div>
                <div class="m-8 self-stretch flex flex-row justify-stretch items-center gap-2">
                    <button class="flex-1 px-4 py-2 bg-gray-200 hover:bg-gray-300 active:bg-gray-400 rounded"> "Save" </button>
                    <button class="flex-1 px-4 py-2 bg-gray-200 hover:bg-gray-300 active:bg-gray-400 rounded"> "Load" </button>
                </div>
            </div>
            <div class="min-h-[100vh] flex-1 flex flex-col justify-start items-stretch">
                <div class="px-8 py-4 sticky top-0 z-30 bg-white flex flex-col justify-start items-start">
                    <h2 class="font-bold text-2xl"> "AIDB" </h2>
                    <h3> { env!("CARGO_PKG_VERSION") } </h3>
                </div>
                <div class="p-8 flex-1 z-0 flex flex-col gap-4 justify-start items-stretch [&>div:first-child>hr]:hidden">
                    <For each=move || { chat().chats.clone() } key=|c| { c.id } children={ |c| { view! {
                        <div class="flex flex-col justify-start">
                            <hr class="my-8 border-slate-100" />
                            <pre class="px-4 py-2 self-end bg-slate-100 rounded-l-xl rounded-br-xl text-wrap break-all ">
                                { c.request.clone() }
                            </pre>
                            <pre class="p-2 self-start text-wrap break-all ">
                                { c.response.clone() }
                            </pre>
                        </div>
                    } } } />
                </div>
                <div class="min-h-40 sticky bottom-0">
                    <div class="min-h-20 mt-12 mb-8 px-8 w-full flex flex-row items-stretch">
                        <div class="px-4 py-2 z-20 flex-1 border-slate-300 border rounded-xl" on:mousedown=move |ev| {
                            ev.prevent_default();
                            focus_input();
                        }>
                            <code class="h-auto text-wrap break-all outline-none" contenteditable node_ref=input_ref on:mousedown=|ev| {
                                ev.stop_propagation();
                            } on:input=move |_| {
                                let input_element = input_ref.get_untracked().unwrap();
                                let mut text = input_element.text_content().unwrap();
                                if text.contains('\u{feff}') {  // first character typed
                                    text.retain(|c| c != '\u{feff}');
                                    input_element.set_text_content(Some(&text));
                                    focus_input();
                                }
                                update_input(text);
                            } on:keydown=move |ev| {
                                if ev.key() == "Enter" {
                                    ev.prevent_default();
                                    let input = input.get_untracked();
                                    if input.is_empty() {
                                        return;
                                    }
                                    let input_element = input_ref.get_untracked().unwrap();
                                    input_element.set_text_content(Some(""));
                                    set_input("".to_owned());
                                    submit_input(input);
                                }
                            } on:paste=move |ev| {
                                ev.stop_propagation();
                                ev.prevent_default();
                                let Some(clipboard) = ev.clipboard_data().and_then(|c| c.get_data("text/plain").ok()) else {
                                    return;
                                };
                                paste_input(clipboard);
                            }>
                                "\u{feff}"  // ZERO WIDTH NO-BREAK SPACE to make caret visible
                            </code>
                            <code> "\u{00a0}" </code>
                            <code class="text-gray-400" on:click=move |_| focus_input()> { hint } </code>
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
