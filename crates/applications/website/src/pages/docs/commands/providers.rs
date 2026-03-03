use crate::components::CodeBlock;
use leptos::*;

#[component]
pub fn CommandProviders() -> impl IntoView {
    view! {
        <div>
            <h1>"gfs providers"</h1>
            <p class="lead">"List available database providers and their supported versions."</p>

            <h2>"Usage"</h2>
            <CodeBlock code="gfs providers [PROVIDER]"/>

            <h2>"Description"</h2>
            <p>"The "<code>"providers"</code>" command displays all supported database providers along with their available versions and features."</p>

            <h2>"Examples"</h2>
            <h3>"List all providers"</h3>
            <CodeBlock code="gfs providers"/>
            <p>"Output:"</p>
            <pre><code>"  database_provider    | version                        | features                                          \n  ---------------------+--------------------------------+---------------------------------------------------\n  mysql                | 8.0, 8.1                       | tls, schema, masking, backup, import              \n  postgres             | 13, 14, 15, 16, 17, 18         | tls, schema, masking, auto-scaling, performance...\n\n  Images are pulled from Docker Hub by default."</code></pre>

            <h3>"Show details for a specific provider"</h3>
            <CodeBlock code="gfs providers postgres"/>

            <h2>"Supported Providers"</h2>
            <h3>"PostgreSQL"</h3>
            <ul>
                <li><strong>"Versions:"</strong>" 13, 14, 15, 16, 17, 18"</li>
                <li><strong>"Features:"</strong>" TLS, schema management, data masking, auto-scaling, performance monitoring"</li>
            </ul>

            <h3>"MySQL"</h3>
            <ul>
                <li><strong>"Versions:"</strong>" 8.0, 8.1"</li>
                <li><strong>"Features:"</strong>" TLS, schema management, data masking, backup, import"</li>
            </ul>

            <h2>"Use Case"</h2>
            <p>"Run this command before initializing a repository to see what database providers and versions are available."</p>

            <h2>"See Also"</h2>
            <ul>
                <li><a href="/docs/commands/init">"gfs init"</a>" - Initialize a repository with a specific provider"</li>
            </ul>
        </div>
    }
}
