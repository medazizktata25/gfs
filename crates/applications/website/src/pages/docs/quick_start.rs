use crate::components::CodeBlock;
use leptos::*;

#[component]
pub fn QuickStart() -> impl IntoView {
    view! {
        <div>
            <h1>"Quick Start"</h1>

            <h2>"1. Check Available Providers"</h2>
            <p>"First, see what database providers are available:"</p>
            <CodeBlock code="gfs providers"/>

            <h2>"2. Create a New Project"</h2>
            <CodeBlock code="mkdir my_project
    cd my_project"/>

            <h2>"3. Initialize the Repository"</h2>
            <CodeBlock code="gfs init --database-provider postgres --database-version 17"/>
            <p>"This creates a "<code>".gfs"</code>" directory and starts a PostgreSQL database in a Docker container."</p>

            <h2>"4. Check Status"</h2>
            <CodeBlock code="gfs status"/>

            <h2>"5. Query Your Database"</h2>
            <p>"Execute SQL directly or open an interactive terminal:"</p>
            <CodeBlock code="gfs query \"SELECT 1\"
    # Or: gfs query (interactive)"/>

            <h2>"6. Make Changes and Commit"</h2>
            <p>"After modifying your database schema or data:"</p>
            <CodeBlock code="gfs commit -m \"my first commit\""/>

            <h2>"7. View Commit History"</h2>
            <CodeBlock code="gfs log"/>

            <h2>"8. Time Travel"</h2>
            <p>"Checkout a previous commit:"</p>
            <CodeBlock code="gfs checkout <commit_hash>"/>

            <h2>"9. Work with Branches"</h2>
            <p>"Create a new branch:"</p>
            <CodeBlock code="gfs checkout -b release"/>
            <p>"Switch back to main:"</p>
            <CodeBlock code="gfs checkout main"/>

            <h2>"Next Steps"</h2>
            <p>"Explore the "<a href="/docs/commands/init">"Commands"</a>" section to learn more about what you can do with GFS."</p>
        </div>
    }
}
