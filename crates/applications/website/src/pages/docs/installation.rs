use crate::components::CodeBlock;
use leptos::*;

#[component]
pub fn Installation() -> impl IntoView {
    view! {
        <div>
            <h1>"Installation"</h1>

            <h2>"Quick Install"</h2>
            <p>"The easiest way to install GFS is using our installation script:"</p>
            <CodeBlock code="curl -fsSL https://gfs.guepard.run/install | bash"/>

            <h2>"Build from Source"</h2>
            <p>"If you prefer to build from source:"</p>
            <CodeBlock code="git clone https://github.com/Guepard-Corp/gfs.git
cd gfs
cargo build --release"/>
            <p>"The binary will be available at "<code>"target/release/gfs"</code>"."</p>

            <h2>"Verify Installation"</h2>
            <p>"After installation, verify that GFS is working:"</p>
            <CodeBlock code="gfs --version"/>

            <h2>"Docker Setup"</h2>
            <p>"GFS requires Docker to be installed and running. Make sure Docker is available before using GFS:"</p>
            <ul>
                <li>"macOS/Windows: Install "<a href="https://www.docker.com/products/docker-desktop/" target="_blank">"Docker Desktop"</a></li>
                <li>"Linux: Install Docker Engine using your distribution's package manager"</li>
            </ul>

            <h2>"Next Steps"</h2>
            <p>"Continue to "<a href="/docs/quick-start">"Quick Start"</a>" to create your first GFS repository."</p>
        </div>
    }
}
