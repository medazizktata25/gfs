use crate::components::{CodeBlock, SchemaDiffTabs};
use leptos::*;

#[component]
pub fn CommandSchema() -> impl IntoView {
    view! {
        <div>
            <h1>"gfs schema"</h1>
            <p class="lead">"Database schema operations: extract, show, and diff."</p>

            <h2>"Usage"</h2>
            <CodeBlock code="# Extract schema from the running database
gfs schema extract [--output <file>] [--compact]

# Show schema from a specific commit
gfs schema show <commit> [--metadata-only] [--ddl-only]

# Compare schemas between two commits
gfs schema diff <commit1> <commit2> [--pretty] [--json]"/>

            <h2>"Subcommands"</h2>
            <h3>"extract"</h3>
            <p>"Extract schema metadata from the running database. Outputs structured JSON with schemas, tables, and columns."</p>
            <ul>
                <li><code>"--output"</code>" - Write to file instead of stdout"</li>
                <li><code>"--compact"</code>" - Output compact JSON (no pretty-printing)"</li>
            </ul>

            <h3>"show"</h3>
            <p>"Show schema from a historical commit. Supports revision refs like "<code>"HEAD"</code>", "<code>"main"</code>", "<code>"HEAD~1"</code>"."</p>
            <ul>
                <li><code>"--metadata-only"</code>" - Show metadata only"</li>
                <li><code>"--ddl-only"</code>" - Show DDL only"</li>
            </ul>

            <h3>"diff"</h3>
            <p>"Compare schemas between two commits. Default output is agentic (line-oriented). Use "<code>"--pretty"</code>" for human-readable format with colors, or "<code>"--json"</code>" for structured output."</p>
            <ul>
                <li><code>"--pretty"</code>" - Human-readable format with visual tree"</li>
                <li><code>"--json"</code>" - Structured JSON output"</li>
                <li><code>"--no-color"</code>" - Disable color output"</li>
            </ul>

            <h2>"Examples"</h2>
            <h3>"Extract schema from running database"</h3>
            <CodeBlock code="gfs schema"/>
            <p>"Output:"</p>
            <pre><code>"{ \"version\": \"PostgreSQL 17\", \"schemas\": [\"public\"],\n  \"tables\": [{ \"name\": \"users\", \"schema\": \"public\" }], ... }"</code></pre>

            <h3>"Extract and save schema"</h3>
            <CodeBlock code="gfs schema extract --output schema.json"/>

            <h3>"Compare with previous commit"</h3>
            <p>"Choose the output format that fits your use case:"</p>
            <SchemaDiffTabs/>

            <h2>"Revision References"</h2>
            <p>"Commands like "<code>"show"</code>" and "<code>"diff"</code>" accept Git-style revision notation:"</p>
            <ul>
                <li><code>"HEAD"</code>" - Current commit"</li>
                <li><code>"main"</code>" - Branch tip"</li>
                <li><code>"HEAD~1"</code>" - Parent of HEAD"</li>
                <li><code>"88b0ff8"</code>" - Short or full commit hash"</li>
            </ul>

            <h2>"See Also"</h2>
            <ul>
                <li><a href="/docs/commands/query">"gfs query"</a>" - Run SQL against the database"</li>
                <li><a href="/docs/commands/checkout">"gfs checkout"</a>" - Switch commits"</li>
                <li><a href="/docs/commands/export">"gfs export"</a>" - Export data"</li>
            </ul>
        </div>
    }
}
