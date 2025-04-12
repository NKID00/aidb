mod app;
use app::App;

use leptos::prelude::*;
use wasm_bindgen::prelude::*;

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
