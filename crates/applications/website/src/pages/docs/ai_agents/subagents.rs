use leptos::*;

#[component]
pub fn AiAgentsSubagents() -> impl IntoView {
    view! {
        <div>
            <h1>"Subagents"</h1>
            <p class="lead">"Specialized AI agents for database querying and schema management."</p>

            <h2>"What are Subagents?"</h2>
            <p>"Subagents are expert AI agents configured with GFS tools and skills. They handle specific database tasks like natural language to SQL conversion, schema-aware querying, and safe schema evolution."</p>

            <h2>"Available Subagents"</h2>
            <h3>"Qwery Agent"</h3>
            <p>"Expert database query agent with schema awareness. Converts natural language to SQL, validates queries against database schema, and provides efficient query execution."</p>
            <ul>
                <li>"Natural language to SQL conversion"</li>
                <li>"Schema-aware query generation and validation"</li>
                <li>"Query optimization and syntax validation"</li>
                <li>"Schema evolution tracking and time-travel queries"</li>
                <li>"Safe destructive operations via GFS branching"</li>
            </ul>
            <p>"Uses the "<code>"use-gfs-mcp"</code>" skill and GFS MCP tools."</p>
            <p><a href="https://github.com/Guepard-Corp/gfs/blob/main/agents/qwery-agent.md" target="_blank">"View Qwery Agent"</a></p>

            <h2>"See Also"</h2>
            <ul>
                <li><a href="/docs/ai-agents/skills">"Skills"</a>" - Prebuilt knowledge for agents"</li>
                <li><a href="/docs/mcp/overview">"MCP Server"</a>" - Tool integration"</li>
            </ul>
        </div>
    }
}
