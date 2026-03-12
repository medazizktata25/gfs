//! Centralized color and output styling for the GFS CLI.
//!
//! Respects TTY detection, NO_COLOR env var, and --color flag.
//! Use the style helpers (cyan, dimmed, etc.) so output respects set_override.

use std::io::IsTerminal;

use owo_colors::OwoColorize;
use owo_colors::Stream;

/// Color mode for CLI output (git-style).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, clap::ValueEnum)]
pub enum ColorMode {
    /// Always use colors (unless NO_COLOR is set)
    Always,
    /// Use colors only when stdout is a TTY and NO_COLOR is not set
    #[default]
    Auto,
    /// Never use colors
    Never,
}

impl ColorMode {
    /// Resolve whether colors should be enabled based on mode, TTY, and NO_COLOR.
    pub fn use_color(self) -> bool {
        if std::env::var("NO_COLOR").is_ok() {
            return false;
        }
        match self {
            ColorMode::Always => true,
            ColorMode::Never => false,
            ColorMode::Auto => std::io::stdout().is_terminal(),
        }
    }

    /// Initialize owo-colors global override. Call once at start of run().
    pub fn init(self) {
        owo_colors::set_override(self.use_color());
    }
}

// ---------------------------------------------------------------------------
// Style helpers - use these instead of .cyan() etc. so set_override is respected.
// Return String to satisfy if_supports_color's 'static lifetime requirement.
// ---------------------------------------------------------------------------

/// Conditionally apply cyan (respects --color, NO_COLOR, TTY).
pub fn cyan(s: impl AsRef<str>) -> String {
    let s = s.as_ref().to_string();
    format!("{}", s.if_supports_color(Stream::Stdout, |t| t.cyan()))
}

/// Conditionally apply dimmed (respects --color, NO_COLOR, TTY).
pub fn dimmed(s: impl AsRef<str>) -> String {
    let s = s.as_ref().to_string();
    format!("{}", s.if_supports_color(Stream::Stdout, |t| t.dimmed()))
}

/// Conditionally apply green (respects --color, NO_COLOR, TTY).
pub fn green(s: impl AsRef<str>) -> String {
    let s = s.as_ref().to_string();
    format!("{}", s.if_supports_color(Stream::Stdout, |t| t.green()))
}

/// Conditionally apply red (respects --color, NO_COLOR, TTY).
pub fn red(s: impl AsRef<str>) -> String {
    let s = s.as_ref().to_string();
    format!("{}", s.if_supports_color(Stream::Stdout, |t| t.red()))
}

/// Conditionally apply yellow (respects --color, NO_COLOR, TTY).
pub fn yellow(s: impl AsRef<str>) -> String {
    let s = s.as_ref().to_string();
    format!("{}", s.if_supports_color(Stream::Stdout, |t| t.yellow()))
}

/// Conditionally apply bold (respects --color, NO_COLOR, TTY).
pub fn bold(s: impl AsRef<str>) -> String {
    let s = s.as_ref().to_string();
    format!("{}", s.if_supports_color(Stream::Stdout, |t| t.bold()))
}

// ---------------------------------------------------------------------------
// Safe printing helpers - handle broken pipe errors gracefully
// ---------------------------------------------------------------------------

use std::io::{self, Write};

/// Safely print a line, handling broken pipe errors gracefully.
/// Returns Ok(()) on success or broken pipe, Err for other errors.
pub fn println_safe(args: std::fmt::Arguments) -> io::Result<()> {
    match writeln!(io::stdout(), "{}", args) {
        Ok(_) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::BrokenPipe => Ok(()),
        Err(e) => Err(e),
    }
}

/// Macro for safe printing that handles broken pipe errors.
/// Use this instead of println! when output may be piped.
#[macro_export]
macro_rules! println_safe {
    () => {
        $crate::output::println_safe(format_args!(""))
    };
    ($($arg:tt)*) => {
        $crate::output::println_safe(format_args!($($arg)*))
    };
}
