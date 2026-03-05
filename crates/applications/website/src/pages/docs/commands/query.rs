use crate::components::CodeBlock;
use leptos::*;

#[component]
pub fn CommandQuery() -> impl IntoView {
    view! {
        <div>
            <h1>"gfs query"</h1>
            <p class="lead">"Execute SQL queries or open an interactive database terminal."</p>

            <h2>"Usage"</h2>
            <CodeBlock code="# Execute a SQL query
gfs query \"SELECT * FROM users LIMIT 3\"

# Open interactive terminal (omit the query)
gfs query"/>

            <h2>"Options"</h2>
            <ul>
                <li><code>"--database"</code>" - Override the default database name from container config"</li>
                <li><code>"--path"</code>" - Path to the GFS repository root (default: current directory)"</li>
            </ul>

            <h2>"Description"</h2>
            <p>"The "<code>"query"</code>" command lets you interact with your GFS-managed database directly from the CLI. No separate database client (e.g. psql, mysql) is required."</p>
            <ul>
                <li>"Execute ad-hoc SQL queries and see results in the terminal"</li>
                <li>"Open an interactive terminal session when no query is provided"</li>
                <li>"Works with PostgreSQL and MySQL (uses native client under the hood)"</li>
            </ul>

            <h2>"Examples"</h2>
            <h3>"Run a SELECT query"</h3>
            <CodeBlock code="gfs query \"SELECT * FROM users LIMIT 3\""/>
            <p>"Output:"</p>
            <pre><code>" id |  name   | email\n----+---------+-------------------\n  1 | Alice   | alice@example.com\n  2 | Bob     | bob@example.com\n(2 rows)"</code></pre>

            <h3>"Create a table"</h3>
            <CodeBlock code="gfs query \"CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);\""/>

            <h3>"Interactive mode"</h3>
            <CodeBlock code="gfs query"/>
            <p>"Opens an interactive database shell. Type SQL and press Enter to execute."</p>

            <h2>"See Also"</h2>
            <ul>
                <li><a href="/docs/commands/status">"gfs status"</a>" - Get connection details"</li>
                <li><a href="/docs/commands/commit">"gfs commit"</a>" - Save changes after querying"</li>
                <li><a href="/docs/commands/schema">"gfs schema"</a>" - Extract and inspect schema"</li>
            </ul>
        </div>
    }
}
