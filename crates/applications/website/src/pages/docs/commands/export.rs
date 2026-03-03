use crate::components::CodeBlock;
use leptos::*;

#[component]
pub fn CommandExport() -> impl IntoView {
    view! {
        <div>
            <h1>"gfs export"</h1>
            <p class="lead">"Export data from the running database to a file."</p>

            <h2>"Usage"</h2>
            <CodeBlock code="gfs export --output-dir <dir> --format <fmt>"/>

            <h2>"Options"</h2>
            <ul>
                <li><code>"--output-dir"</code>" (required) - Directory where the export file will be written (created if absent)"</li>
                <li><code>"--format"</code>" (required) - Export format: sql (plain-text) or custom (PostgreSQL binary)"</li>
                <li><code>"--path"</code>" - Path to the GFS repository root"</li>
            </ul>

            <h2>"Supported Formats (PostgreSQL)"</h2>
            <ul>
                <li><strong>"sql"</strong>" - Plain-text SQL dump (pg_dump --format=plain). Output: export.sql"</li>
                <li><strong>"custom"</strong>" - PostgreSQL custom binary format. Output: export.dump"</li>
            </ul>

            <h2>"Examples"</h2>
            <h3>"Export to current directory as SQL"</h3>
            <CodeBlock code="gfs export --output-dir . --format sql"/>

            <h3>"Export to backup directory"</h3>
            <CodeBlock code="gfs export --output-dir /backups/my-repo --format custom"/>

            <h2>"Output"</h2>
            <p>"On success, prints the absolute path to the exported file:"</p>
            <pre><code>"Exported to /Users/project/export.sql"</code></pre>

            <h2>"See Also"</h2>
            <ul>
                <li><a href="/docs/commands/import">"gfs import"</a>" - Import data from a file"</li>
                <li><a href="/docs/commands/status">"gfs status"</a>" - Check container is running"</li>
            </ul>
        </div>
    }
}
