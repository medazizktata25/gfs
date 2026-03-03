use leptos::*;

#[component]
pub fn GettingStarted() -> impl IntoView {
    view! {
        <div>
            <h1>"Getting Started with GFS"</h1>
            <p class="lead">"GFS (Git For database Systems) brings Git-like version control to your databases."</p>

            <h2>"What is GFS?"</h2>
            <p>"GFS enables you to:"</p>
            <ul>
                <li><strong>"Commit"</strong>" database states with meaningful messages"</li>
                <li><strong>"Branch"</strong>" and "<strong>"merge"</strong>" database schemas and data"</li>
                <li><strong>"Time travel"</strong>" through your database history"</li>
                <li><strong>"Collaborate"</strong>" on database changes with confidence"</li>
                <li><strong>"Rollback"</strong>" to any previous state instantly"</li>
            </ul>

            <div class="alert warning">
                <strong>"⚠️ Important Notice"</strong>
                <p>"This project is under active development and not yet suitable for production use. Expect breaking changes, incomplete features, and evolving APIs."</p>
            </div>

            <h2>"Supported Databases"</h2>
            <ul>
                <li>"PostgreSQL (versions 13-18)"</li>
                <li>"MySQL (versions 8.0-8.1)"</li>
            </ul>

            <h2>"Requirements"</h2>
            <ul>
                <li>"Docker (latest version recommended)"</li>
                <li>"Bash/Zsh shell"</li>
                <li><code>"curl"</code>" for installation"</li>
                <li><code>"tar"</code>" for extracting releases"</li>
            </ul>

            <h2>"Next Steps"</h2>
            <p>"Continue to "<a href="/docs/installation">"Installation"</a>" to set up GFS on your system."</p>
        </div>
    }
}
