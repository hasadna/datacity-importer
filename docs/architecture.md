# Architecture

## How the image is assembled

The base image is `akariv/dgp-app:latest`, built from `~/Code/datariver/dgp/dgp-app/Dockerfile`:

- `python:3.12-slim`, `WORKDIR /app`, running as user `etl`
- `pip install dgp-server` plus dgp-app's `requirements*.txt`. These pull in `dgp`, `dataflows`,
  `tableschema-sql`, `apache-airflow[postgres]<3`, `geocoder`, `pyproj` and others.
- `ADD . .`, so `/app` contains `server.py`, `server_dgp.py`, `dags/`, `etl_server/`,
  `taxonomies/` (a `sample` taxonomy), `ui/dist/…`, and `entrypoint.sh`.

This repo's `Dockerfile`, line by line:

| Line | Effect |
|---|---|
| `COPY requirements.dev.txt .` + `RUN sudo pip install -U -r requirements.dev.txt` | Adds `dataflows-ckan` (used by the `muni_business_kinds` operator) and `pyquery` (used by `HTMLTableURLExtractor` in `taxonomies/loaders.py`). It needs `sudo` because the base image's user is `etl`. |
| `COPY configuration.json dags/` | **Replaces** `/app/dags/configuration.json`. `server.py` reads it at startup and serves it to the UI. See [configuration.json](#configurationjson) below. |
| `COPY logo.png ui/dist/ui/assets/logo.png` | Overrides the UI logo. |
| `COPY taxonomies taxonomies` | **Merges** into `/app/taxonomies`. `index.yaml` is replaced, so only DataCity taxonomies are registered. The base image's `taxonomies/sample/` stays on disk but nothing references it. |
| `COPY datacity_server datacity_server` | `/app/datacity_server`: shared processors imported by every `process.py`. |
| `COPY operators dags/operators/` | **Merges** into `/app/dags/operators/`. Adds `business_licensing_law` and `muni_business_kinds` next to the base image's `dgp_kind` (the operator that runs all DataCity pipelines) and `printer`. |
| `COPY setup.py .` + `RUN pip install .` | Effectively a no-op. `setup.py` declares `py_modules=['datacity_server']`, but there is no `datacity_server.py`: it's a package. Building the wheel produces only `dist-info` (verified). |
| `ENV PYTHONPATH="/app/"` | This is what makes `datacity_server` importable (added in `6ed24f9`, 2026-06-26). Taxonomy modules are loaded from file paths (`spec_from_file_location`) and do `from datacity_server.processors import …`. Under Airflow/gunicorn entry-point scripts, `/app` isn't on `sys.path` otherwise. |

## Runtime processes

`/app/entrypoint.sh` (from dgp-app) starts:

- `airflow db migrate`, `airflow scheduler`, and `airflow webserver`
- `gunicorn server_dgp:app` on `127.0.0.1:5001`. This is dgp-server: the mapping workbench API,
  where sources are analyzed and previewed.
- `gunicorn server:app` on `0.0.0.0:5000`. This is the Flask UI and API: pipelines, users,
  taxonomies, files and auth. It proxies the workbench.

The working directory is `/app` for all of them. Many paths are relative to the working directory:

- `taxonomies/index.yaml`
- `dags/configuration.json`
- `.cache/` and `.checkpoints/` (reference-data and source caches)
- `out/` (written by `muni_business_kinds`)

Environment variables the code reads:

| Variable | Used by |
|---|---|
| `ETLS_DATABASE_URL` | Pipeline definitions (`etl_pipeline`), taxonomy overrides (`etl_taxonomies`), and the other `etl_server` tables. |
| `DATABASE_URL` / `AUTH_DATABASE_URL` | Users and auth (dgp-oauth2). |
| `DATASETS_DATABASE_URL` | **Output tables**, one per taxonomy, written by `taxonomies/publish.py`. Also read by `GeoCoder` (as a cache) and both operators. |
| `AIRFLOW__…` | Airflow metadata DB and settings. |
| `BUCKET_NAME`, `S3_ENDPOINT_URL`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` | Uploaded files (`FileLoaderDGP`, `etl_server.files`). |
| `EXTERNAL_ADDRESS` | OAuth callback base URL. |
| `GOOGLE_MAPS_API_KEY` | Optional. When it's set, `GeoCoder` uses Google instead of the DataCity Nominatim server. |
| `CKAN_HOST`, `CKAN_API_KEY` | The `muni_business_kinds` operator. |

## Pipeline lifecycle

1. **Define (UI).** A user creates a pipeline of kind `dgp_kind` (shown as "DataCity") and sets
   its source URL or upload. The workbench (dgp-server) runs the dgp *analyzers*: file format,
   sheet, headers, skip rows, taxonomy guess and column mapping. The resulting configuration is
   saved as `params.dgpConfig` in `etl_pipeline`. That includes `taxonomy.columnTypes`, a **copy**
   of the taxonomy's column types at that moment.
   - The workbench doesn't read column types straight from `taxonomies/*/column_types.json`.
     On startup, `server_dgp.py` seeds the `etl_taxonomies` table from the files with
     `ON CONFLICT DO NOTHING`. From then on, **the DB copy wins** and overwrites the in-memory
     registry. To change column types in the UI, you have to edit `etl_taxonomies` (through the
     taxonomies API/admin UI or SQL), or delete that row so it's re-seeded on the next restart.
2. **Schedule.** `dags/dag_fetcher.py` creates one Airflow DAG per pipeline. Each DAG has a single
   `PythonOperator` that calls `operators.<kind>.operator(name, params, pipeline)`, so the kind
   name *is* the operator module name. A description containing `depends on: <dag-id>` adds an
   `ExternalTaskSensor`.
3. **Run.** `dags/operators/dgp_kind` (from dgp-app):
   - sets `publish.allowed = True`, then writes `extra.metadata` (title, description, dag_id,
     timestamps) and applies `extra.*` params
   - skips the run if `extra.metadata.disabled`
   - runs `SimpleDGP` with these steps:
     `PreLoaderDGP → FileLoaderDGP → LoaderDGP → PostLoaderDGP → TransformDGP → EnricherDGP → PublisherDGP`.
     - `PostLoaderDGP` is the `_common_` `loading_module` (`taxonomies/loaders.py`), whose analyzers
       run during analysis.
     - `EnricherDGP` is the taxonomy's own `processing_module` (`<taxonomy>/process.py`).
     - `PublisherDGP` is the `_common_` `publishing_module` (`taxonomies/publish.py`), because no
       taxonomy defines its own.
   - Each enricher module returns `(preflow, postflow)`. Pre-flows (for example, loading reference
     datapackages) are inserted at the front of the whole flow. Post-flows run in order after the
     transform.
4. **Frozen config.** With `publish.allowed = True`, `TaxonomyFetcherAnalyzer` does **not**
   refresh `taxonomy.columnTypes`. A run always uses the column types frozen in that pipeline's
   saved config. Editing `column_types.json` (or `etl_taxonomies`) changes nothing for existing
   pipelines until each one is re-analyzed and saved in the UI. Code in `process.py`, `publish.py`
   and `datacity_server/` takes effect on the next deploy.
5. **Publish.** `taxonomies/publish.py` writes to `DATASETS_DATABASE_URL`, table
   `<taxonomy id with - → _>`. It first deletes that source's earlier rows. See
   [taxonomies.md § DB tables](taxonomies.md#db-tables-and-re-runs).

Without `publish.allowed` (workbench previews and the local notebook), `LoaderDGP` limits the source
to 5000 rows and caches it under `.cache/<hash>`.

## configuration.json

This file replaces dgp-app's `dags/configuration.json` entirely. `etl_server` serves it to the UI
through `Controllers.configuration(admin)`, which removes kinds marked `"admin": true` for non-admin
users.

- **`kinds`**: the pipeline types a user can create. `name` must match a module under
  `dags/operators/`.
  - `dgp_kind` displays as **"DataCity"**. `"dgp": true` means it uses the DGP mapping workbench.
    `fields: []` means no extra form fields. All taxonomy pipelines use this kind.
  - `business_licensing_law` displays as "Business Licensing Law". It's `dgp: false` and
    `admin: true`, and runs `operators/business_licensing_law`.
  - `muni_business_kinds` displays as "Business Kind Processing". It's `dgp: false` and
    `admin: true`, and runs `operators/muni_business_kinds`.
- **`theme`**: UI colours (`primary`, `primary_dark`, `secondary`).
- **`homepage`**: `https://www.datacity.org.il/`, the link behind the header logo (the default is `/dashboard`).
- **Texts**: `taskDescription` is the Hebrew prompt for a pipeline's description field ("תיאור
  המאגר - בפרט שם הרשות, סוג המידע ושנה רלוונטית"). dgp-app's own file also has
  `taskNameDescription`; DataCity's doesn't, so no help text appears under the name field. There's no `defaultLocale`,
  so the server's default (`en`) applies. The Hebrew UI texts are the taxonomy and column-type
  titles, which come from the taxonomies.
- **`dateFormats`**: the date-parsing options shown in the mapper. `auto`, `auto_ymd`, `auto_dmy`,
  `auto_ydm` and `auto_mdy` are handled by `TransformDGP.datetime_handler` (dateutil with different
  `dayfirst`/`yearfirst` settings). This is a subset of dgp-app's list: there's no `default`, no
  `%d/%m/%Y`, and so on.
- **`features`**: `forcePrivate: false` makes every pipeline non-private and hides the visibility toggle in the edit form (`true` would force private; leaving it unset shows the toggle). dgp-app's
  `titleEditable`/`titleVisible` and its `dataRecords` section aren't set.
