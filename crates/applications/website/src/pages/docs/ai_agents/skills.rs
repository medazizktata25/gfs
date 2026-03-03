use leptos::*;

#[component]
pub fn AiAgentsSkills() -> impl IntoView {
    view! {
        <div>
            <h1>"Skills"</h1>
            <p class="lead">"Prebuilt skills that teach AI agents how to use GFS effectively."</p>

            <h2>"What are Skills?"</h2>
            <p>"Skills are reusable knowledge packages that AI agents can load to understand and operate GFS. They provide structured instructions, examples, and best practices for database version control."</p>

            <h2>"Available Skills"</h2>
            <h3>"use-gfs-cli"</h3>
            <p>"Git-like version control for databases using the GFS CLI. Covers commits, branches, time travel, and schema versioning."</p>
            <ul>
                <li>"Installation and quick start"</li>
                <li>"Core commands: init, commit, checkout, log, status"</li>
                <li>"Schema operations: extract, show, diff"</li>
                <li>"Query, export, and import"</li>
            </ul>
            <p><a href="https://github.com/Guepard-Corp/gfs/blob/main/skills/use-gfs-cli/SKILL.md" target="_blank">"View use-gfs-cli skill"</a></p>

            <h3>"use-gfs-mcp"</h3>
            <p>"GFS MCP Server for AI agent integration. Provides Model Context Protocol tools for database version control with automatic schema versioning."</p>
            <ul>
                <li>"MCP configuration and setup"</li>
                <li>"All GFS operations exposed as tools"</li>
                <li>"Revision references for schema and diff"</li>
                <li>"Integration with Claude Desktop and other MCP clients"</li>
            </ul>
            <p><a href="https://github.com/Guepard-Corp/gfs/blob/main/skills/use-gfs-mcp/SKILL.md" target="_blank">"View use-gfs-mcp skill"</a></p>

            <h2>"See Also"</h2>
            <ul>
                <li><a href="/docs/ai-agents/subagents">"Subagents"</a>" - Specialized agents for database tasks"</li>
                <li><a href="/docs/mcp/overview">"MCP Server"</a>" - Programmatic access to GFS"</li>
            </ul>
        </div>
    }
}
