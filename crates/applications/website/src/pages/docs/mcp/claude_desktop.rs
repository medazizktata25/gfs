use crate::components::CodeBlock;
use leptos::*;

const CLAUDE_CONFIG_PATH_MACOS: &str =
    "~/Library/Application Support/Claude/claude_desktop_config.json";
const CLAUDE_CONFIG_PATH_WIN: &str = "%APPDATA%/Claude/claude_desktop_config.json";
const CLAUDE_CONFIG_PATH_LINUX: &str = "~/.config/Claude/claude_desktop_config.json";

#[component]
pub fn McpClaudeDesktop() -> impl IntoView {
    view! {
        <div>
            <h1>"Claude Desktop Integration"</h1>
            <p class="lead">"Configure GFS as an MCP server in Claude Desktop for AI-powered database version control."</p>

            <h2>"Prerequisites"</h2>
            <ul>
                <li>"GFS CLI installed (run "<code>"gfs version"</code>" to verify)"</li>
                <li>"Claude Desktop with MCP support"</li>
            </ul>

            <h2>"Configuration"</h2>
            <p>"Add GFS to your Claude Desktop config. The config file location depends on your OS:"</p>
            <ul>
                <li><strong>"macOS"</strong>" - " {CLAUDE_CONFIG_PATH_MACOS}</li>
                <li><strong>"Windows"</strong>" - " {CLAUDE_CONFIG_PATH_WIN}</li>
                <li><strong>"Linux"</strong>" - " {CLAUDE_CONFIG_PATH_LINUX}</li>
            </ul>

            <h3>"Basic Configuration"</h3>
            <CodeBlock code=super::CLAUDE_CONFIG_BASIC.to_string()/>

            <h3>"With Repository Path"</h3>
            <p>"To target a specific GFS repository:"</p>
            <CodeBlock code=super::CLAUDE_CONFIG_WITH_PATH.to_string()/>

            <h2>"Auto-Configuration via Install Script"</h2>
            <p>"When you install GFS with the official script, it can auto-configure Claude Desktop if detected:"</p>
            <CodeBlock code="curl -fsSL https://gfs.guepard.run/install | bash".to_string()/>
            <p>"Select Claude when prompted. The script adds GFS to "<code>"mcpServers"</code>" and copies the use-gfs-cli and use-gfs-mcp skills to "<code>"~/.claude/skills/"</code>"."</p>

            <h2>"Restart Claude Desktop"</h2>
            <p>"After editing the config, restart Claude Desktop. GFS tools will appear and Claude can commit, checkout, query, and manage your database with full version control."</p>

            <h2>"Screenshots"</h2>
            <p>"GFS MCP server in Claude Desktop:"</p>
            <img src="/public/assets/claude-desktop-gfs-mcp.png" alt="GFS MCP in Claude Desktop" class="docs-screenshot"/>
            <p>"GFS tools available to Claude:"</p>
            <img src="/public/assets/claude-desktop-gfs-tools.png" alt="GFS tools in Claude Desktop" class="docs-screenshot"/>

            <h2>"See Also"</h2>
            <ul>
                <li><a href="/docs/mcp/overview">"MCP Overview"</a>" - Available tools"</li>
                <li><a href="/docs/mcp/claude-code">"Claude Code"</a>" - IDE extension"</li>
                <li><a href="/docs/ai-agents/skills">"Skills"</a>" - use-gfs-mcp skill"</li>
            </ul>
        </div>
    }
}
