use crate::components::CodeBlock;
use leptos::*;

#[component]
pub fn CommandCheckout() -> impl IntoView {
    view! {
        <div>
            <h1>"gfs checkout"</h1>
            <p class="lead">"Switch to a different commit or branch."</p>

            <h2>"Usage"</h2>
            <CodeBlock code="# Checkout a specific commit
gfs checkout <COMMIT_HASH>

# Create and checkout a new branch
gfs checkout -b <BRANCH_NAME>

# Checkout an existing branch
gfs checkout <BRANCH_NAME>"/>

            <h2>"Options"</h2>
            <ul>
                <li><code>"-b"</code>" - Create a new branch and switch to it"</li>
            </ul>

            <h2>"Description"</h2>
            <p>"The "<code>"checkout"</code>" command allows you to:"</p>
            <ul>
                <li>"Travel back to any previous database state"</li>
                <li>"Switch between branches"</li>
                <li>"Create new branches from the current state"</li>
            </ul>
            <p>"When you checkout a commit or branch, GFS restores your database to that exact state."</p>

            <h2>"Examples"</h2>
            <h3>"Checkout a specific commit"</h3>
            <CodeBlock code="gfs checkout 88b0ff8"/>
            <p>"Your database will be restored to the state at that commit."</p>

            <h3>"Create a new branch"</h3>
            <CodeBlock code="gfs checkout -b feature-test"/>
            <p>"Output:"</p>
            <pre><code>"Switched to new branch 'feature-test' (88b0ff8)"</code></pre>

            <h3>"Switch to an existing branch"</h3>
            <CodeBlock code="gfs checkout main"/>
            <p>"Switches back to the "<code>"main"</code>" branch."</p>

            <h2>"How It Works"</h2>
            <ol>
                <li>"GFS stops the current database container"</li>
                <li>"The database storage is restored to the target commit"</li>
                <li>"A new container starts with the restored state"</li>
                <li>"You can now work with the database at that point in history"</li>
            </ol>

            <h2>"Time Travel Example"</h2>
            <CodeBlock code="# View your commits
gfs log

# Go back to a previous commit
gfs checkout 88b0ff8

# Verify the database is restored
gfs query \"SELECT 1\"

# Return to the latest state
gfs checkout main"/>

            <h2>"Working with Branches"</h2>
            <CodeBlock code="# Create a branch for experimental changes
gfs checkout -b experiment

# Make changes to your database
# ...

# Commit your changes
gfs commit -m \"Experimental schema changes\"

# Switch back to main
gfs checkout main

# Your database is back to main's state
# The experimental changes are preserved in the experiment branch"/>

            <h2>"Important Notes"</h2>
            <ul>
                <li>"Any uncommitted changes will be lost when checking out"</li>
                <li>"The database container is recreated during checkout"</li>
                <li>"Active connections to the database will be closed"</li>
            </ul>

            <h2>"See Also"</h2>
            <ul>
                <li><a href="/docs/commands/log">"gfs log"</a>" - View commit history to find commit hashes"</li>
                <li><a href="/docs/commands/commit">"gfs commit"</a>" - Save changes before checking out"</li>
                <li><a href="/docs/commands/status">"gfs status"</a>" - Check current branch"</li>
                <li><a href="/docs/commands/query">"gfs query"</a>" - Execute SQL or open interactive terminal"</li>
            </ul>
        </div>
    }
}
