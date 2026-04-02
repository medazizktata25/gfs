use crate::components::CodeBlock;
use leptos::*;

#[component]
pub fn CommandCompute() -> impl IntoView {
    view! {
        <div>
            <h1>"gfs compute"</h1>
            <p class="lead">"Manage the database container (start, stop, status, logs)."</p>

            <h2>"Usage"</h2>
            <CodeBlock code="gfs compute <ACTION> [--path <dir>]"/>

            <h2>"Subcommands"</h2>
            <ul>
                <li><code>"start"</code>" - Start the database container"</li>
                <li><code>"stop"</code>" - Stop the database container"</li>
                <li><code>"restart"</code>" - Restart the container"</li>
                <li><code>"status"</code>" - Show container status"</li>
                <li><code>"pause"</code>" - Pause the container"</li>
                <li><code>"unpause"</code>" - Unpause the container"</li>
                <li><code>"logs"</code>" - View container logs"</li>
                <li><code>"config <KEY> <VALUE>"</code>" - Read or write a compute config value"</li>
            </ul>

            <h2>"Logs Options"</h2>
            <p>"For "<code>"gfs compute logs"</code>":"</p>
            <ul>
                <li><code>"--tail"</code>" - Number of lines to show from the end"</li>
                <li><code>"--since"</code>" - Show logs since a timestamp"</li>
                <li><code>"--stdout"</code>" / "<code>"--stderr"</code>" - Toggle stdout/stderr"</li>
            </ul>

            <h2>"Compute Config Keys"</h2>
            <p>"Use "<code>"gfs compute config <KEY> <VALUE>"</code>" to update runtime settings. Changes take effect after "<code>"gfs compute restart"</code>"."</p>
            <ul>
                <li><code>"db.port"</code>" - Host port bound to the database container (e.g., "<code>"5432"</code>"). Useful when you need the database reachable on a fixed port."</li>
            </ul>

            <h2>"Examples"</h2>
            <h3>"Start the database"</h3>
            <CodeBlock code="gfs compute start"/>

            <h3>"View recent logs"</h3>
            <CodeBlock code="gfs compute logs --tail 50"/>

            <h3>"Change the host port and apply it"</h3>
            <CodeBlock code={"gfs compute config db.port 5432\ngfs compute restart"}/>

            <h2>"See Also"</h2>
            <ul>
                <li><a href="/docs/commands/status">"gfs status"</a>" - Repository and compute status"</li>
                <li><a href="/docs/commands/init">"gfs init"</a>" - Initialize and start the database"</li>
            </ul>
        </div>
    }
}
