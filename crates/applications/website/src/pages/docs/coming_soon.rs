use leptos::*;

#[component]
pub fn ComingSoon(#[prop(into)] page: String) -> impl IntoView {
    view! {
        <div>
            <h1>"Page Not Found"</h1>
            <p>"The page "<code>{page}</code>" was not found."</p>
            <p><a href="/docs">"Return to documentation"</a></p>
        </div>
    }
}
