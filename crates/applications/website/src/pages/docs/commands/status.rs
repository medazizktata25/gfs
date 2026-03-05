use crate::components::CodeBlock;
use leptos::*;

#[component]
pub fn CommandStatus() -> impl IntoView {
    view! {
        <div>
            <h1>"gfs status"</h1>
            <p class="lead">"Show the current state of storage and compute resources."</p>

            <h2>"Usage"</h2>
            <CodeBlock code="gfs status"/>

            <h2>"Description"</h2>
            <p>"The "<code>"status"</code>" command displays information about your GFS repository, including:"</p>
            <ul>
                <li>"Current branch"</li>
                <li>"Database connection information"</li>
                <li>"Docker container status"</li>
                <li>"Storage backend information"</li>
                <li>"Compute resource status"</li>
            </ul>

            <h2>"Example Output"</h2>
            <pre><code>"  Repository\n  ────────────────────────────────────────\n  Branch               main\n  Active workspace     .gfs/workspaces/main/0/data\n\n  Compute\n  ────────────────────────────────────────\n  Provider             postgres\n  Version              17\n  Status               ● running\n  Container ID         37f65464d421…\n  Container data dir   .gfs/workspaces/main/0/data\n  Connection           postgresql://postgres:postgres@localhost:55251/postgres"</code></pre>

            <h2>"Use Cases"</h2>
            <ul>
                <li>"Check if the database container is running"</li>
                <li>"Get connection details for your database"</li>
                <li>"Verify which branch you're currently on"</li>
                <li>"Troubleshoot connection issues"</li>
            </ul>

            <h2>"See Also"</h2>
            <ul>
                <li><a href="/docs/commands/init">"gfs init"</a>" - Initialize a repository"</li>
                <li><a href="/docs/commands/log">"gfs log"</a>" - View commit history"</li>
            </ul>
        </div>
    }
}
