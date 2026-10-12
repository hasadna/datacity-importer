# datacity-importer

This repo deploys the DataCity data-import app at **https://app.datacity.org.il**.

It doesn't hold an application of its own. The Docker image is `FROM akariv/dgp-app:latest`,
and this repo layers DataCity-specific pieces on top:

- the UI/kinds configuration
- the taxonomies (target schemas such as municipal budgets, schools and businesses)
- the enrichment processors
- two admin-only Airflow operators

Users point the app at a municipal file (Excel, CSV, JSON, HTML table and so on) and map its
columns to a taxonomy. Airflow then runs the pipeline and writes rows into one Postgres table
per taxonomy, in `DATASETS_DATABASE_URL`.

## The stack

| Layer | Where | What it does |
|---|---|---|
| **dgp** (library) | `~/Code/datariver/dgp/dgp`, PyPI `dgp` 0.1.3 | The "data genus processor" pipeline: load → analyze structure → map to a taxonomy → enrich (`processing_module`) → publish (`publishing_module`). |
| **dgp-server** | `~/Code/datariver/dgp/dgp-server`, PyPI `dgp-server` 0.0.95 | aiohttp API used by the mapping workbench, plus `publish_flow` (the DB writer used by `taxonomies/publish.py`). |
| **dgp-app** | `~/Code/datariver/dgp/dgp-app`, image `akariv/dgp-app:latest` | Flask UI/API (`server.py`, port 5000), dgp-server (`server_dgp.py`, 127.0.0.1:5001), Airflow scheduler and webserver, the `dgp_kind` operator, and the `etl_server` package (pipelines, users, taxonomies, files). |
| **this repo** | `hasadna/datacity-importer` | `configuration.json`, `taxonomies/`, `datacity_server/`, `operators/`, `logo.png`. |

## Repo layout

```
Dockerfile               image = dgp-app + the files below (see docs/architecture.md)
configuration.json       → /app/dags/configuration.json : kinds, theme, texts, date formats
taxonomies/
  index.yaml             the taxonomy registry (ids, titles, column types, modules)
  loaders.py             _common_ loading module: SharepointJson, HTMLTableURLExtractor
  publish.py             _common_ publishing module: MissingColumnsAdder, EnsureUniqueFields, DBWriter
  muni-budgets/          column_types.json, process.py, chapters/ + types/ reference datapackages
  <other taxonomies>/    column_types.json + process.py
datacity_server/
  processors/            shared enrichers: municipality name→code, filters, dedupe, geocoding
operators/               admin-only Airflow operators → /app/dags/operators/
  business_licensing_law/
  muni_business_kinds/
.github/workflows/main.yml   CI: build, push, bump hasadna-k8s
docker-compose.yml, migrate.py, trigger.py, "Local Development.ipynb"   local/legacy tooling (partly stale)
backup/                  2020 export of pipeline configurations (used by migrate.py)
```

## Documentation

- [docs/architecture.md](docs/architecture.md): how the image is assembled, the runtime processes,
  the pipeline lifecycle (UI → Airflow → DB), and `configuration.json`.
- [docs/taxonomies.md](docs/taxonomies.md): how taxonomies work in dgp, every taxonomy in
  `index.yaml`, the `_common_` loaders and publisher, and the resulting DB tables and re-run semantics.
- [docs/muni-budgets.md](docs/muni-budgets.md): the budget taxonomy in detail (card codes,
  classification, direction, phases, values, municipality lookup), including behaviour verified by
  running the pipeline locally.
- [docs/operators-and-processors.md](docs/operators-and-processors.md): `operators/` and
  `datacity_server/processors`.
- [docs/deployment-and-local-dev.md](docs/deployment-and-local-dev.md): CI/CD, local development,
  and what's stale.
- [docs/gotchas.md](docs/gotchas.md): the short list of things that bite.

## Deploying, in one paragraph

Push to `main`. GitHub Actions builds the image and pushes
`docker.pkg.github.com/hasadna/datacity-dgp-importer/server:<sha>` (and `:latest`). It then
commits `dgpAppImage: …:<sha>` to `hasadna/hasadna-k8s` in
`apps/datacity/values-hasadna-auto-updated.yaml`, and the cluster picks it up from there. Put
`--no-deploy` in the commit message to build without bumping k8s. See
[docs/deployment-and-local-dev.md](docs/deployment-and-local-dev.md).

## Secrets

The working copy usually contains untracked secrets: `.env`, `datacity_deploy_key*`,
`deploy-key*` and `temp_key*`. Only `.env` is in `.gitignore`. Never `git add -A` / `git add .`
here, and never paste these files' contents into docs, issues or chat.
