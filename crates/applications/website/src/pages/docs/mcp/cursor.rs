use crate::components::CodeBlock;
use leptos::*;

const CURSOR_CONFIG_PATH_GLOBAL: &str = "~/.cursor/mcp.json";
const CURSOR_CONFIG_PATH_PROJECT: &str = ".cursor/mcp.json";

#[component]
pub fn McpCursor() -> impl IntoView {
    view! {
        <div>
            <h1>"Cursor Integration"</h1>
            <p class="lead">"Configure GFS as an MCP server in Cursor for AI-powered database version control in your IDE."</p>

            <h2>"Prerequisites"</h2>
            <ul>
                <li>"GFS CLI installed (run "<code>"gfs version"</code>" to verify)"</li>
                <li>"Cursor IDE"</li>
            </ul>

            <h2>"Configuration"</h2>
            <p>"Cursor supports two MCP config locations:"</p>
            <ul>
                <li><strong>"Global"</strong>" - " {CURSOR_CONFIG_PATH_GLOBAL}" - Applies across all workspaces"</li>
                <li><strong>"Project"</strong>" - " {CURSOR_CONFIG_PATH_PROJECT}" - In your project root, shared with collaborators"</li>
            </ul>

            <h3>"Basic Configuration (Global)"</h3>
            <p>"Edit "<code>{CURSOR_CONFIG_PATH_GLOBAL}</code>" and add the "<code>"mcpServers"</code>" section (or merge into existing):"</p>
            <CodeBlock code=super::CLAUDE_CONFIG_BASIC.to_string()/>

            <h3>"With Repository Path"</h3>
            <p>"To target a specific GFS repository:"</p>
            <CodeBlock code=super::CLAUDE_CONFIG_WITH_PATH.to_string()/>

            <h3>"Project-Level Configuration"</h3>
            <p>"Add "<code>{CURSOR_CONFIG_PATH_PROJECT}</code>" in your project root to share GFS with your team. The format is the same as above."</p>

            <h3>"UI Method"</h3>
            <p>"Alternatively, open Cursor Settings (Cmd+, on macOS, Ctrl+, on Windows) → Tools & MCP → Add new MCP server, then enter the command and args."</p>

            <h2>"Auto-Configuration via Install Script"</h2>
            <p>"When you install GFS with the official script, it can auto-configure Cursor if detected:"</p>
            <CodeBlock code="curl -fsSL https://gfs.guepard.run/install | bash".to_string()/>
            <p>"Select Cursor when prompted. The script adds GFS to "<code>"mcpServers"</code>" and copies the use-gfs-cli and use-gfs-mcp skills to "<code>"~/.cursor/skills/"</code>"."</p>

            <h2>"Restart Cursor"</h2>
            <p>"After editing the config, completely restart Cursor for MCP servers to load. GFS tools will appear and Cursor can commit, checkout, query, and manage your database with full version control."</p>

            <h2>"Screenshot"</h2>
            <p>"GFS MCP server in Cursor:"</p>
            <img src="/public/assets/cursor-gfs-mcp.png" alt="GFS MCP in Cursor" class="docs-screenshot"/>

            <h2>"See Also"</h2>
            <ul>
                <li><a href="/docs/mcp/overview">"MCP Overview"</a>" - Available tools"</li>
                <li><a href="/docs/mcp/claude-desktop">"Claude Desktop"</a>" - Standalone app"</li>
                <li><a href="/docs/mcp/claude-code">"Claude Code"</a>" - VS Code extension"</li>
                <li><a href="/docs/mcp/http-mode">"HTTP Mode"</a>" - Daemon and HTTP transport"</li>
                <li><a href="/docs/ai-agents/skills">"Skills"</a>" - use-gfs-mcp skill"</li>
            </ul>
        </div>
    }
}
