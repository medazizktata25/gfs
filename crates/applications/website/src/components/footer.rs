use leptos::*;

#[component]
pub fn Footer() -> impl IntoView {
    view! {
        <footer class="footer">
            <div class="container">
                <div class="footer-content">
                    <div class="footer-section">
                        <h3>"Product"</h3>
                        <ul>
                            <li><a href="/docs">"Documentation"</a></li>
                            <li><a href="https://github.com/Guepard-Corp/gfs" target="_blank">"GitHub"</a></li>
                            <li><a href="https://github.com/Guepard-Corp/gfs/blob/main/CHANGELOG.md" target="_blank">"Changelog"</a></li>
                            <li><a href="https://github.com/Guepard-Corp/gfs/blob/main/ROADMAP.md" target="_blank">"Roadmap"</a></li>
                        </ul>
                    </div>
                    <div class="footer-section">
                        <h3>"Community"</h3>
                        <ul>
                            <li><a href="https://discord.gg/SEdZuJbc5V" target="_blank">"Discord"</a></li>
                            <li><a href="https://github.com/Guepard-Corp/gfs/issues" target="_blank">"Issues"</a></li>
                            <li><a href="https://github.com/Guepard-Corp/gfs/blob/main/CONTRIBUTING.md" target="_blank">"Contributing"</a></li>
                            <li><a href="https://youtu.be/WlOkLnoY2h8" target="_blank">"Demo Video"</a></li>
                        </ul>
                    </div>
                    <div class="footer-section">
                        <h3>"Legal"</h3>
                        <ul>
                            <li><a href="https://github.com/Guepard-Corp/gfs/blob/main/LICENCE" target="_blank">"License (MIT)"</a></li>
                            <li><a href="https://github.com/Guepard-Corp/gfs/blob/main/CODE_OF_CONDUCT.md" target="_blank">"Code of Conduct"</a></li>
                            <li><a href="https://github.com/Guepard-Corp/gfs/blob/main/SECURITY.md" target="_blank">"Security"</a></li>
                        </ul>
                    </div>
                    <div class="footer-section">
                        <h3>"Company"</h3>
                        <ul>
                            <li><a href="https://guepard.run" target="_blank">"Guepard"</a></li>
                            <li><a href="https://github.com/Guepard-Corp/gfs/blob/main/THANK-YOU.md" target="_blank">"Acknowledgments"</a></li>
                        </ul>
                    </div>
                </div>
                <div class="footer-bottom">
                    <p>"Made with ❤️ by the Guepard team"</p>
                    <p class="footer-version">{format!("Version {}", env!("CARGO_PKG_VERSION"))}</p>
                </div>
            </div>
        </footer>
    }
}
