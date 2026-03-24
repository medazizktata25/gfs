use crate::components::CodeBlock;
use leptos::*;

#[component]
pub fn CommandInit() -> impl IntoView {
    view! {
        <div>
            <h1>"gfs init"</h1>
            <p class="lead">"Initialize a new GFS repository."</p>

            <h2>"Usage"</h2>
            <CodeBlock code="gfs init --database-provider <PROVIDER> --database-version <VERSION> [--port <PORT>]"/>

            <h2>"Options"</h2>
            <ul>
                <li><code>"--database-provider"</code>" (required) - Database type (e.g., "<code>"postgres"</code>", "<code>"mysql"</code>")"</li>
                <li><code>"--database-version"</code>" (required) - Database version (e.g., "<code>"17"</code>" for PostgreSQL, "<code>"8.0"</code>" for MySQL)"</li>
                <li><code>"--port"</code>" (optional) - Host port to bind for the database container (e.g., "<code>"5432"</code>"). Defaults to Docker auto-assigning a free port."</li>
            </ul>

            <h2>"Description"</h2>
            <p>"The "<code>"init"</code>" command creates a new GFS repository in the current directory. It:"</p>
            <ul>
                <li>"Creates a "<code>".gfs"</code>" directory to store repository metadata"</li>
                <li>"Starts a Docker container with the specified database"</li>
                <li>"Initializes the database for version control"</li>
                <li>"Creates an initial commit (root commit)"</li>
            </ul>

            <h2>"Examples"</h2>
            <h3>"Initialize with PostgreSQL 17"</h3>
            <CodeBlock code="gfs init --database-provider postgres --database-version 17"/>

            <h3>"Initialize with MySQL 8.0"</h3>
            <CodeBlock code="gfs init --database-provider mysql --database-version 8.0"/>

            <h3>"Initialize on a fixed host port"</h3>
            <CodeBlock code="gfs init --database-provider postgres --database-version 17 --port 5432"/>

            <h2>"What Happens"</h2>
            <ol>
                <li>"A "<code>".gfs"</code>" directory is created in your current directory"</li>
                <li>"Docker pulls the specified database image if not already available"</li>
                <li>"A Docker container starts with the database"</li>
                <li>"The database is configured for GFS version control"</li>
                <li>"Connection information is displayed"</li>
            </ol>

            <h2>"Query Your Database"</h2>
            <p>"Use "<code>"gfs query"</code>" to run SQL or open an interactive session. No separate database client needed:"</p>
            <CodeBlock code="gfs query \"SELECT 1\"
    gfs query  # interactive terminal"/>

            <h2>"Requirements"</h2>
            <ul>
                <li>"Docker must be installed and running"</li>
                <li>"The current directory should be empty or not already a GFS repository"</li>
                <li>"Sufficient disk space for the database container"</li>
            </ul>

            <h2>"See Also"</h2>
            <ul>
                <li><a href="/docs/commands/providers">"gfs providers"</a>" - List available providers and versions"</li>
                <li><a href="/docs/commands/status">"gfs status"</a>" - Check repository status"</li>
            </ul>
        </div>
    }
}
