KidsPlaza Budget Allocation Pipeline powers the Digital Advertising Data Analysis process at KidsPlaza by automating the ingestion, transformation and loading of digital advertising data into our centralized Google BigQuery warehouse.

It integrates Budget Allocation Insights data, supporting both daily sync and historical backfills. The pipeline is structured around modular stages such as fetch, enrich, ingest, staging, mart, making it easier to test, maintain and extend.

This repository is maintained by the Digital Marketing Team at KidsPlaza. For questions, access requests, or contributions, please contact the team via email at quang.nn@kidsplaza.vn (internal) or nhatquang.nguyen.129@gmail.com (external), or reach out via the internal Slack channel #data-engineering.

⚠️ Disclaimer: This project is intended for internal use only. As a result, output schemas, metrics, and aggregations may not align with how other organizations manage Budget Allocation data. Users are encouraged to review and adapt downstream transformations including enrichment, staging and materialization to fit their own businesss requirements.

📄 License: You are free to use, modify, and distribute the source code for personal, educational, or commercial purposes, provided that the original copyright notice and license are included.

🤖 AI-Assisted Development: This repository includes code, documentation, and architectural guidance that has been partially developed or enhanced using AI tools (e.g., GitHub Copilot, ChatGPT by OpenAI), under the supervision of the development team. All AI-generated output has been reviewed and adapted to meet KidsPlaza's internal production standards.