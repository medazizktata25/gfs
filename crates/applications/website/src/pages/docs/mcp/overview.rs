use leptos::*;

#[component]
pub fn McpOverview() -> impl IntoView {
    view! {
        <div>
            <h1>"MCP Server Overview"</h1>
            <p class="lead">"The GFS MCP server exposes database version control operations as tools for AI assistants and automation."</p>

            <h2>"What is MCP?"</h2>
            <p>"The Model Context Protocol (MCP) is a standard that lets AI assistants and tools interact with external systems. GFS provides an MCP server so that Claude, Cursor, and other MCP clients can perform repository and compute operations without invoking the CLI directly."</p>

            <h2>"Available Tools"</h2>
            <p>"The GFS MCP server exposes 13 tools that mirror the CLI:"</p>
            <ul>
                <li><code>"list_providers"</code>" - List supported database providers and versions"</li>
                <li><code>"status"</code>" - Repository and compute status (branch, container, connection)"</li>
                <li><code>"commit"</code>" - Create a commit with message"</li>
                <li><code>"log"</code>" - View commit history"</li>
                <li><code>"checkout"</code>" - Switch branch or checkout commit"</li>
                <li><code>"init"</code>" - Initialize a new GFS repository"</li>
                <li><code>"compute"</code>" - Container lifecycle (start, stop, restart, logs)"</li>
                <li><code>"export_database"</code>" - Export data to file"</li>
                <li><code>"import_database"</code>" - Import data from file"</li>
                <li><code>"query"</code>" - Execute SQL against the database"</li>
                <li><code>"extract_schema"</code>" - Extract schema from running database"</li>
                <li><code>"show_schema"</code>" - Show schema from a specific commit"</li>
                <li><code>"diff_schema"</code>" - Compare schemas between two commits"</li>
            </ul>

            <h2>"Transports"</h2>
            <p>"The server supports two modes:"</p>
            <ul>
                <li><strong>"Stdio (default)"</strong>" - For direct client integration (Claude Desktop, Cursor). The client spawns "<code>"gfs mcp"</code>" and communicates over stdin/stdout."</li>
                <li><strong>"HTTP"</strong>" - For daemon mode or remote access. Run "<code>"gfs mcp start"</code>" or "<code>"gfs mcp web"</code>" to listen on port 3000."</li>
            </ul>

            <h2>"Repository Path"</h2>
            <p>"Tools accept an optional "<code>"path"</code>" parameter. When omitted, the server uses "<code>"GFS_REPO_PATH"</code>" or the current working directory at startup."</p>

            <h2>"See Also"</h2>
            <ul>
                <li><a href="/docs/mcp/claude-desktop">"Claude Desktop"</a>" - Configure GFS with Claude"</li>
                <li><a href="/docs/mcp/claude-code">"Claude Code"</a>" - IDE extension (VS Code)"</li>
                <li><a href="/docs/mcp/cursor">"Cursor"</a>" - Cursor IDE"</li>
                <li><a href="/docs/mcp/http-mode">"HTTP Mode"</a>" - Daemon and HTTP transport"</li>
                <li><a href="/docs/ai-agents/skills">"Skills"</a>" - use-gfs-mcp skill for agents"</li>
            </ul>
        </div>
    }
}
