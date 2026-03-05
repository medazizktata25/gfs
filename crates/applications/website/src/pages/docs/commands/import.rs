use crate::components::CodeBlock;
use leptos::*;

#[component]
pub fn CommandImport() -> impl IntoView {
    view! {
        <div>
            <h1>"gfs import"</h1>
            <p class="lead">"Import data from a file into the running database."</p>

            <h2>"Usage"</h2>
            <CodeBlock code="gfs import --file <path> [--format <fmt>]"/>

            <h2>"Options"</h2>
            <ul>
                <li><code>"--file"</code>" (required) - Path to the dump file"</li>
                <li><code>"--format"</code>" - Import format (inferred from file extension when omitted)"</li>
                <li><code>"--path"</code>" - Path to the GFS repository root"</li>
            </ul>

            <h2>"Supported Formats (PostgreSQL)"</h2>
            <ul>
                <li><strong>"sql"</strong>" - Plain-text SQL file (.sql extension)"</li>
                <li><strong>"custom"</strong>" - PostgreSQL binary dump (.dump extension)"</li>
                <li><strong>"csv"</strong>" - CSV file (.csv extension)"</li>
            </ul>

            <h2>"Format Inference"</h2>
            <p>"When "<code>"--format"</code>" is omitted, format is inferred from the file extension."</p>

            <h2>"Examples"</h2>
            <h3>"Import a SQL file"</h3>
            <CodeBlock code="gfs import --file ./backup.sql"/>

            <h3>"Import a CSV file"</h3>
            <CodeBlock code="gfs import --file ./data.csv --format csv"/>

            <h2>"Output"</h2>
            <p>"On success, prints the absolute path to the imported file:"</p>
            <pre><code>"Imported from /Users/project/backup.sql"</code></pre>

            <h2>"See Also"</h2>
            <ul>
                <li><a href="/docs/commands/export">"gfs export"</a>" - Export data to a file"</li>
                <li><a href="/docs/commands/status">"gfs status"</a>" - Check container is running"</li>
            </ul>
        </div>
    }
}
