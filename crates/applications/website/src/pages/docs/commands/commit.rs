use crate::components::CodeBlock;
use leptos::*;

#[component]
pub fn CommandCommit() -> impl IntoView {
    view! {
        <div>
            <h1>"gfs commit"</h1>
            <p class="lead">"Commit the current database state."</p>

            <h2>"Usage"</h2>
            <CodeBlock code="gfs commit -m <MESSAGE> [--author <name>] [--author-email <email>]"/>

            <h2>"Options"</h2>
            <ul>
                <li><code>"-m, --message"</code>" (required) - Commit message describing the changes"</li>
                <li><code>"--author"</code>" - Override the author name (fallback: repo config → global config → git config → \"user\")"</li>
                <li><code>"--author-email"</code>" - Override the author email (fallback: repo config → global config → git config)"</li>
            </ul>

            <h2>"Description"</h2>
            <p>"The "<code>"commit"</code>" command creates a snapshot of your current database state, including:"</p>
            <ul>
                <li>"Schema changes (tables, columns, indexes, constraints)"</li>
                <li>"Data changes (inserts, updates, deletes)"</li>
                <li>"Database configuration"</li>
            </ul>

            <h2>"Examples"</h2>
            <h3>"Commit with a message"</h3>
            <CodeBlock code="gfs commit -m \"Add users table\""/>
            <p>"Output:"</p>
            <pre><code>"[main] 88b0ff8  Add users table"</code></pre>

            <h3>"Commit schema changes"</h3>
            <CodeBlock code="gfs commit -m \"Add email column to users table\""/>

            <h3>"Commit data changes"</h3>
            <CodeBlock code="gfs commit -m \"Import initial user data\""/>

            <h2>"How It Works"</h2>
            <ol>
                <li>"GFS captures a complete snapshot of your database"</li>
                <li>"The snapshot is stored efficiently using deduplication"</li>
                <li>"A commit hash is generated"</li>
                <li>"The commit is added to the current branch's history"</li>
            </ol>

            <h2>"Best Practices"</h2>
            <ul>
                <li>"Write clear, descriptive commit messages"</li>
                <li>"Commit logical units of change"</li>
                <li>"Test your changes before committing"</li>
                <li>"Commit frequently to maintain detailed history"</li>
            </ul>

            <h2>"See Also"</h2>
            <ul>
                <li><a href="/docs/commands/config">"gfs config"</a>" - Set default author identity"</li>
                <li><a href="/docs/commands/log">"gfs log"</a>" - View commit history"</li>
                <li><a href="/docs/commands/checkout">"gfs checkout"</a>" - Switch to a different commit"</li>
                <li><a href="/docs/commands/status">"gfs status"</a>" - Check repository status"</li>
            </ul>
        </div>
    }
}
