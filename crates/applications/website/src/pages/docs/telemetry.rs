use leptos::*;

#[component]
pub fn Telemetry() -> impl IntoView {
    view! {
        <div>
            <h1>"Telemetry"</h1>
            <p>
                "GFS collects anonymous usage data to help improve the product. \
                 No personally identifiable information is ever collected."
            </p>

            <h2>"What is collected"</h2>
            <p>"The following data is sent to our analytics backend (PostHog) on each command invocation:"</p>
            <ul>
                <li><strong>"command"</strong>" — the name of the subcommand (e.g. "<code>"commit"</code>", "<code>"log"</code>")"</li>
                <li><strong>"source"</strong>" — the surface that triggered the command: "<code>"cli"</code>", "<code>"mcp"</code>", "<code>"cursor"</code>", "<code>"claude_code"</code>", or "<code>"ci"</code></li>
                <li><strong>"version"</strong>" — the GFS CLI version"</li>
                <li><strong>"os"</strong>" — the operating system (e.g. "<code>"macos"</code>", "<code>"linux"</code>")"</li>
                <li><strong>"error_category"</strong>" — on failure, a coarse error type (e.g. "<code>"RepositoryError"</code>") — never the full message"</li>
            </ul>

            <h2>"What is NOT collected"</h2>
            <ul>
                <li>"Commit messages or branch names"</li>
                <li>"File or repository paths"</li>
                <li>"Author names or email addresses"</li>
                <li>"SQL queries or database contents"</li>
                <li>"Database connection strings"</li>
                <li>"Full error messages"</li>
                <li>"Any user-identifiable information"</li>
            </ul>

            <h2>"Anonymous identifier"</h2>
            <p>
                "On first run, GFS generates a random UUID and stores it at "<code>"~/.gfs/telemetry_id"</code>". \
                 This identifier is anonymous — it is not linked to your name, email, or any account. \
                 It exists solely to distinguish unique installations and count active users."
            </p>

            <h2>"How to opt out"</h2>
            <p>"You can disable telemetry at any time using either of these methods:"</p>

            <h3>"Via config"</h3>
            <pre><code>"gfs config --global telemetry.enabled false"</code></pre>
            <p>"To re-enable:"</p>
            <pre><code>"gfs config --global telemetry.enabled true"</code></pre>
            <p>"Check the current setting:"</p>
            <pre><code>"gfs config --global telemetry.enabled"</code></pre>

            <h3>"Via environment variable"</h3>
            <p>
                "Set "<code>"GFS_NO_TELEMETRY=1"</code>" in your shell environment to disable telemetry for the current session \
                 or permanently by adding it to your shell profile:"
            </p>
            <pre><code>"export GFS_NO_TELEMETRY=1"</code></pre>
            <p>"The environment variable takes precedence over the config file setting."</p>
        </div>
    }
}
