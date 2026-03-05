use crate::components::CodeBlock;
use leptos::*;

const CLAUDE_CODE_CONFIG_PATH_USER: &str = "~/.claude.json";
const CLAUDE_CODE_CONFIG_PATH_PROJECT: &str = ".mcp.json";

#[component]
pub fn McpClaudeCode() -> impl IntoView {
    view! {
        <div>
            <h1>"Claude Code Integration"</h1>
            <p class="lead">"Configure GFS as an MCP server in Claude Code (VS Code, Cursor) for AI-powered database version control in your IDE."</p>

            <h2>"Prerequisites"</h2>
            <ul>
                <li>"GFS CLI installed (run "<code>"gfs version"</code>" to verify)"</li>
                <li>"Claude Code extension in VS Code or Cursor"</li>
            </ul>

            <h2>"Configuration"</h2>
            <p>"Claude Code supports two MCP config locations:"</p>
            <ul>
                <li><strong>"User scope"</strong>" - " {CLAUDE_CODE_CONFIG_PATH_USER}" - Applies across all projects"</li>
                <li><strong>"Project scope"</strong>" - " {CLAUDE_CODE_CONFIG_PATH_PROJECT}" - In your repo root, shared with collaborators"</li>
            </ul>

            <h3>"Basic Configuration (User)"</h3>
            <p>"Edit "<code>"~/.claude.json"</code>" and add the "<code>"mcpServers"</code>" section (or merge into existing):"</p>
            <CodeBlock code=super::CLAUDE_CONFIG_BASIC.to_string()/>

            <h3>"With Repository Path"</h3>
            <p>"To target a specific GFS repository:"</p>
            <CodeBlock code=super::CLAUDE_CONFIG_WITH_PATH.to_string()/>

            <h3>"Project-Level Configuration"</h3>
            <p>"Add "<code>{CLAUDE_CODE_CONFIG_PATH_PROJECT}</code>" in your project root to share GFS with your team. The format is the same as above."</p>

            <h2>"Restart Claude Code"</h2>
            <p>"After editing the config, restart the Claude Code session (or reload the window). GFS tools will appear and Claude can commit, checkout, query, and manage your database with full version control."</p>

            <h2>"Screenshot"</h2>
            <p>"GFS MCP server in Claude Code:"</p>
            <img src="/public/assets/claude-code-mcp.png" alt="GFS MCP in Claude Code" class="docs-screenshot"/>

            <h2>"See Also"</h2>
            <ul>
                <li><a href="/docs/mcp/overview">"MCP Overview"</a>" - Available tools"</li>
                <li><a href="/docs/mcp/claude-desktop">"Claude Desktop"</a>" - Standalone app"</li>
                <li><a href="/docs/mcp/cursor">"Cursor"</a>" - Cursor IDE"</li>
                <li><a href="/docs/mcp/http-mode">"HTTP Mode"</a>" - Daemon and HTTP transport"</li>
                <li><a href="/docs/ai-agents/skills">"Skills"</a>" - use-gfs-mcp skill"</li>
            </ul>
        </div>
    }
}
