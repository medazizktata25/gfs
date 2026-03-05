mod claude_code;
mod claude_desktop;
mod cursor;
mod http_mode;
mod overview;

pub use claude_code::McpClaudeCode;
pub use claude_desktop::McpClaudeDesktop;
pub use cursor::McpCursor;
pub use http_mode::McpHttpMode;
pub use overview::McpOverview;

// Shared MCP config snippets used across claude_desktop, claude_code, and cursor.
const CLAUDE_CONFIG_BASIC: &str = r#"{
  "mcpServers": {
    "gfs": {
      "command": "gfs",
      "args": ["mcp"]
    }
  }
}"#;

const CLAUDE_CONFIG_WITH_PATH: &str = r#"{
  "mcpServers": {
    "gfs": {
      "command": "gfs",
      "args": ["mcp", "--path", "/path/to/your/repo"]
    }
  }
}"#;
