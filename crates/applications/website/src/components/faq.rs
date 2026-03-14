use leptos::*;

#[derive(Clone)]
struct FaqItem {
    question: &'static str,
    answer: &'static str,
}

#[component]
pub fn Faq() -> impl IntoView {
    let faqs = vec![
        FaqItem {
            question: "What databases does GFS support?",
            answer: "GFS currently supports PostgreSQL (versions 13-18) and MySQL (versions 8.0-8.1).",
        },
        FaqItem {
            question: "Is GFS ready for production?",
            answer: "GFS is for local use only. If you need a production ready database versioning system, check https://app.guepard.run",
        },
        FaqItem {
            question: "How does GFS work?",
            answer: "GFS uses Docker to manage isolated database environments and creates snapshots of your database state at each commit, allowing you to travel through history and work with branches just like Git.",
        },
        FaqItem {
            question: "Do I need Docker?",
            answer: "Yes, Docker is required to run GFS as it manages database containers for isolation and versioning.",
        },
        FaqItem {
            question: "Can I use GFS with my existing database?",
            answer: "GFS creates and manages its own database instances. You can import data from existing databases into a GFS-managed repository.",
        },
        FaqItem {
            question: "Is GFS open source?",
            answer: "Yes, GFS is licensed under the MIT license and the source code is available on GitHub.",
        },
        FaqItem {
            question: "Can I use GFS with AI coding agents?",
            answer: "Yes, GFS is designed to work with AI coding agents. It provides a Git-like interface for managing database changes, allowing agents to create branches, roll back, and avoid data loss. GFS also brings productivity tools for agents such as importing, exporting, and querying data without consuming extra tokens for such repetitive actions.",
        },
        FaqItem {
            question: "Can I use GFS in my company?",
            answer: "Yes, GFS is free to use for personal and commercial purposes. You can use it in your company without any limitations. If you need a production ready database versioning system, check https://guepard.run. Guepard Platform is a commercial database versioning system that is designed to be used in production workflows.",
        },
        FaqItem {
            question: "What is the difference between GFS and Guepard Platform?",
            answer: "Guepard Platform provides a Cloud and On-Premise database versioning system. It support a large number of databases and versions and provides a control plante and more enterprise features for teams and organizations. GFS is a local database versioning system that is designed to be used for development and testing purposes. If you are a developer or a small team, GFS is a great choice. If you are a large team or an organization that needs support and more enterprise features, Guepard Platform is a great choice.",
        },
    ];

    let (expanded, set_expanded) = create_signal::<Option<usize>>(None);

    view! {
        <section class="faq-section">
            <div class="container">
                <h2 class="section-title">"Frequently Asked Questions"</h2>
                <div class="faq-list">
                    {faqs.into_iter().enumerate().map(|(idx, faq)| {
                        view! {
                            <div class="faq-item">
                                <button
                                    class="faq-question"
                                    on:click=move |_| {
                                        if expanded.get() == Some(idx) {
                                            set_expanded.set(None)
                                        } else {
                                            set_expanded.set(Some(idx))
                                        }
                                    }
                                >
                                    <span>{faq.question}</span>
                                    <span class="faq-icon">
                                        {move || if expanded.get() == Some(idx) { "−" } else { "+" }}
                                    </span>
                                </button>
                                <div class=move || {
                                    if expanded.get() == Some(idx) {
                                        "faq-answer expanded"
                                    } else {
                                        "faq-answer"
                                    }
                                }>
                                    <p>{faq.answer}</p>
                                </div>
                            </div>
                        }
                    }).collect::<Vec<_>>()}
                </div>
            </div>
        </section>
    }
}
