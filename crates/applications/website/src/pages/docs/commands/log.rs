use crate::components::CodeBlock;
use leptos::*;

#[component]
pub fn CommandLog() -> impl IntoView {
    view! {
        <div>
            <h1>"gfs log"</h1>
            <p class="lead">"Show the commit history."</p>

            <h2>"Usage"</h2>
            <CodeBlock code="gfs log"/>

            <h2>"Description"</h2>
            <p>"The "<code>"log"</code>" command displays the commit history of the current branch, showing:"</p>
            <ul>
                <li>"Commit hash (short form)"</li>
                <li>"Commit message"</li>
                <li>"Author information"</li>
                <li>"Timestamp"</li>
                <li>"Branch information"</li>
            </ul>

            <h2>"Example Output"</h2>
            <pre><code>"commit 88b0ff8 (HEAD -> main, main)\nAuthor: user\nDate:   Sun Mar  1 12:56:43 2026 +0000\n\n    Add users table"</code></pre>

            <h2>"Understanding the Output"</h2>
            <ul>
                <li><strong>"commit hash"</strong>" - Unique identifier for the commit"</li>
                <li><strong>"HEAD"</strong>" - Current commit you're on"</li>
                <li><strong>"branch name"</strong>" - Branch this commit belongs to"</li>
                <li><strong>"Author"</strong>" - Who made the commit"</li>
                <li><strong>"Date"</strong>" - When the commit was made"</li>
                <li><strong>"message"</strong>" - Description of the changes"</li>
            </ul>

            <h2>"Use Cases"</h2>
            <ul>
                <li>"Review what changes were made to the database"</li>
                <li>"Find a specific commit to checkout"</li>
                <li>"Understand the evolution of your database schema"</li>
                <li>"Debug when a change was introduced"</li>
            </ul>

            <h2>"See Also"</h2>
            <ul>
                <li><a href="/docs/commands/commit">"gfs commit"</a>" - Create a new commit"</li>
                <li><a href="/docs/commands/checkout">"gfs checkout"</a>" - Switch to a different commit"</li>
            </ul>
        </div>
    }
}
