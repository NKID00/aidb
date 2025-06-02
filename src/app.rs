use leptos::{html, logging::log, prelude::*};

#[derive(Debug, Clone)]
struct Chat {
    version: usize,
    request: String,
    response: String,
}

impl Chat {
    pub fn new(request: String) -> Self {
        Self {
            version: 0,
            request,
            response: "".to_owned(),
        }
    }

    pub fn version(&self) -> usize {
        self.version
    }

    pub fn request(&self) -> &str {
        &self.request
    }

    pub fn response(&self) -> &str {
        &self.response
    }

    pub fn respond(&mut self, response: impl AsRef<str>) {
        self.response += response.as_ref();
        self.version += 1;
    }
}

#[component]
pub fn App() -> impl IntoView {
    let (chat, set_chat) = signal(Vec::<Chat>::new());
    let (input, set_input) = signal(String::new());
    let (hint, set_hint) = signal("SQL input".to_string());
    let input_element = NodeRef::<html::Span>::new();

    for i in 0..100 {
        set_chat.update_untracked(|v| {
            v.push({
                let mut c = Chat::new(format!("Chat {}", i));
                c.respond(format!("Response {}", i));
                c
            })
        });
    }

    Effect::new(move |_| {
        log!("sql: {:?}", input());
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

    view! {
        <div class="flex-1 flex flex-row w-full items-start">
            <div class="w-[40%] h-[100vh] sticky top-0 bg-sky-50 flex justify-center items-center">
                <h2> "OPFS Explorer" </h2>
            </div>
            <div class="flex-1 flex flex-col justify-start items-stretch">
                <div class="px-8 py-4 sticky top-0 z-30 bg-white flex flex-col justify-start items-start">
                    <h2 class="font-bold text-2xl"> "AIDB" </h2>
                    <h3> { env!("CARGO_PKG_VERSION") } </h3>
                </div>
                <div class="p-8 flex-1 z-0 flex flex-col gap-4 justify-start items-stretch [&>div:first-child>hr]:hidden">
                    <For each=chat key=Chat::version children={ |c| { view! {
                        <div class="flex flex-col justify-start">
                            <hr class="my-8 border-slate-100" />
                            <div class="px-4 py-2 self-end bg-slate-100 rounded-l-xl rounded-br-xl">
                                { c.request().to_owned() }
                            </div>
                            <div class="p-2 self-start">
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
                                set_input(text.replace('\u{a0}', " ").to_owned());
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
