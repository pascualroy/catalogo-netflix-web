Purpose & context
Ángel is building a personal streaming catalog management system, currently focused on Netflix, with plans to expand to other platforms (Prime, Disney+, Max). The project's goal is a fully searchable local catalog enriched with metadata, poster images, and IMDb data. The system runs across two machines: a desktop PC used for development and running the crawler, and an always-on laptop hosting the production MariaDB database (netflix_catalogo) and serving the web viewer. Ángel uses a locally-running Devstral Small 2 24B model via Ollama (on an RTX 5080 16GB GPU) for semantic data extraction during crawling.
Current state
The project has undergone a major schema redesign: separate peliculas and series tables were unified into a single titulos table, with shared relational tables (titulos_personas, titulos_generos, titulos_idiomas). This enables cross-content searches by actor, genre, and language. The migration script is migracion_v3.sql.
The main crawler (main.py, formerly netflix_crawler_devstral.py) writes to the unified schema, downloads posters inline, and has a corrected content-type classification logic using a decision matrix combining _es_serie and es_doc signals. A companion script corregir_tipos.py retroactively fixes misclassified titles using num_temporadas as the sole reliable series indicator.
The Flask web viewer (visor_catalogo.py) has been updated to the unified schema and features autocomplete search for actors/persons, genres, and audio languages (loaded once at startup, filtered client-side with accent-normalized matching). Android TV compatibility is in place via <a href="#"> card wrappers.
IMDb integration is underway: imdb_importar.py downloads and imports the seven public IMDb TSV datasets into a local imdb database (filtered for relevant title types, Hispanic regions, and acting/directing roles). imdb_personas.py matches Netflix personas entries to IMDb nconst identifiers, adding imdb_id, birth_year, and death_year columns, with a two-mode startup menu (fully automatic vs. manual review of ambiguous matches).
On the horizon

Complete and stabilize IMDb integration
Add full series support with season/episode data (acknowledged as significantly more complex; plan is to analyze Netflix series page reactContext structure before designing schema and crawler)
Normalize schema further before adding additional streaming platforms
Build a unified cross-platform HTML viewer with search across all platforms
Explore IMDb data enrichment for titles (via title.akas.tsv.gz filtered by region, joined on Spanish title + year to resolve tconst)

Key learnings & principles

num_temporadas is the only reliable signal for detecting series; genre keywords are not trustworthy for this purpose
Netflix does not include a title's own BOXSHOT in its own page HTML — it only appears on other titles' pages that recommend it (drove decision to abandon inline thumbnail capture)
MariaDB's consistent snapshot behavior under autocommit = False causes stale results; cursor recreation and explicit conn.commit() per iteration are required for background scripts that need to see newly inserted rows
MariaDB ENUM fields silently ignore invalid values (insert empty string), so ENUM values must be used exactly
GPU inference (~4 seconds/title) vs. CPU-only (~4 minutes/title) has a major practical impact on crawler scheduling; natural inference time reduces the need for long artificial pauses
executemany bulk inserts on Windows with NULL values can fail; row-by-row fallback resolves this

Approach & patterns

Iterative and pragmatic: Ángel reviews designs before coding, simplifies scope when complexity exceeds personal-use needs (e.g., dropped episode-level detail for series initially), and validates logic against real data
Prefers clean production code over defensive checks once setup is complete (e.g., removing column-creation migration checks after columns exist)
Catches logical errors by testing against real data and corrects them directly
Gradual exploration of new ideas before committing to implementation

Tools & resources

Languages/frameworks: Python, Flask, MariaDB
AI inference: Ollama with Devstral Small 2 24B (devstral-small-2:24b), Ollama API via /api/chat endpoint
Hardware: Desktop PC with RTX 5080 16GB GPU (development/crawling); always-on laptop (production DB + web viewer)
Data sources: Netflix static HTML + window.netflix.reactContext JSON; IMDb public TSV datasets (datasets.imdbws.com)
MariaDB tuning: innodb_buffer_pool_size=1500M, innodb_flush_log_at_trx_commit=2, innodb_flush_method=O_DIRECT, query_cache_size=64M (configured for 4GB RAM machine)
Environment: Windows with PowerShell (development)
