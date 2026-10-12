# Deployment and local development

## CI/CD (`.github/workflows/main.yml`)

The workflow runs on a push to `main` (GitHub's default branch for `hasadna/datacity-importer`)
and on `workflow_dispatch`. It runs on `ubuntu-22.04`:

1. Logs in to `docker.pkg.github.com` with `GITHUB_TOKEN`.
2. Pulls `akariv/dgp-app:latest`. The base image is **never pinned**: every build picks up
   whatever dgp-app was pushed last.
3. Builds with `--cache-from …/server:latest` when that image exists. The image name is
   `docker.pkg.github.com/hasadna/datacity-dgp-importer/server`, the repo's old name.
4. Pushes `:<GITHUB_SHA>`. On `main` it also pushes `:latest`.
5. Unless the last commit message contains `--no-deploy`:
   - clones `hasadna/hasadna-k8s` using the `HASADNA_K8S_DEPLOY_KEY` secret
   - runs `python update_yaml.py '{"dgpAppImage":"<image>:<sha>"}' apps/datacity/values-hasadna-auto-updated.yaml`
   - commits it as "Datacity DGP CI" and pushes to `master`

   The cluster rolls out from there.

To check what's deployed, look at the `dgpAppImage` key in `hasadna-k8s`. On 2026-10-11 it was
`…/server:6ed24f98…`, the current `HEAD` of `main`.

The local clone's `origin/HEAD` points at a stale `master` branch. Work on `main`.

## Local development

### Running the full app (`docker-compose.yml`)

`docker compose up` starts:
- `db`: `akariv/dgp-app-db:latest` (Postgres), published on host port **55432**
- `server`: built from this repo, port 5000, with `ETLS_DATABASE_URL`, `DATABASE_URL`,
  `DATASETS_DATABASE_URL` and the Airflow DB all pointing at the `db` service

This setup is partly stale:
- There's no S3 configuration (`BUCKET_NAME`, `S3_ENDPOINT_URL`, `AWS_*`), so file uploads and
  `FileLoaderDGP` won't work.
- There's no OAuth configuration, so you can't log in.
- It uses `AIRFLOW__CORE__SQL_ALCHEMY_CONN`. Airflow 2.3+ reads `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`;
  the old name is deprecated.
- The commented-out volume points at an old `dataspot/etl-server` path.

### Running one production pipeline locally (`Local Development.ipynb`)

This is the practical way to debug a taxonomy. The notebook:

1. Loads `.env` (untracked). It needs `DATABASE_URL_ETLS`: a connection string to the
   **production** ETLs database. Treat it as a secret with full access.
2. Fetches the pipeline record by `PIPELINE_ID` (from the app's URL) and extracts
   `params.dgpConfig`.
3. Sets `publish.allowed = False`, fills in metadata, and applies `extra.*` params.
4. Runs `FileLoaderDGP → LoaderDGP → PostLoaderDGP → TransformDGP → EnricherDGP`.
   **`PublisherDGP` is commented out**, so nothing is written to the DB. The output goes to
   `output_dataset/` with `dump_to_path`.

How this differs from production:
- `publish.allowed=False` means the source is capped at **5000 rows** and cached in `.cache/<hash>`
  (delete it to re-fetch).
- `TaxonomyFetcherAnalyzer` **re-reads column types from the repo files**, while production uses
  the column types frozen in the pipeline config.
- `DATASETS_DATABASE_URL` is set to `sqlite:///datasets.sqlite`. It's only needed because
  `publish.py` creates its engine when imported.

Requirements:
- The notebook says `pip install etl-server`. That PyPI package (0.0.13, 2022) is an older
  incarnation of `etl_server`. dgp-app's in-repo `etl_server` is what runs in production, so
  installing dgp-app from `~/Code/datariver/dgp/dgp-app` (`pip install -e .`) is closer.
- You also need `dgp`, `dgp-server`, `dataflows`, `pyquery`, `geocoder`, `pyproj`, `ijson`,
  `python-dotenv`, and `PYTHONPATH=.` for `datacity_server`.
- `FileLoaderDGP` needs the S3 variables if the pipeline's source is an uploaded file.

The untracked `.checkpoints/`, `.cache/`, `out/`, `output_dataset/` and `datasets.sqlite` in the
repo root are leftovers from these runs.

### Minimal harness without the ETLs DB

To exercise a taxonomy end to end against a local Postgres, mirror what the `dgp_kind` operator
does:

1. Build a `Config` with `source`, `taxonomy.id` and optionally `constants`.
2. Run `SimpleDGP(...).analyze()` once with `publish.allowed` unset. This fills in headers, the
   mapping and `taxonomy.columnTypes`.
3. Adjust `model.mapping[*].columnType` as needed.
4. Set `publish.allowed=True`, rebuild `SimpleDGP` with
   `PreLoaderDGP, LoaderDGP, PostLoaderDGP, TransformDGP, EnricherDGP, PublisherDGP`, then
   `analyze()` and `flow().process()`.

Run it with `cwd` containing `taxonomies/` (a symlink is fine), with `PYTHONPATH` set to the repo
root and `DATASETS_DATABASE_URL` pointing at the DB. Use Postgres rather than SQLite: SQLite fails
on insert ("row value misused") with this schema, and it doesn't enforce the same primary-key and
NOT NULL behaviour.

## Legacy and stale tooling

| File | Status |
|---|---|
| `migrate.py` | A 2020 one-off that recreated pipelines from `backup/datapackage.json` and started each one 180 s apart. It's **stale**: it creates pipelines with `kind='datacity'`, which isn't a kind (the kind is `dgp_kind`), so their DAGs would fail to import. It also reads users from the ETLs DB. |
| `trigger.py` | Starts **every** pipeline, sleeping 180 s between them. It still matches the `etl_server` API, but think twice: every run deletes and rewrites its source's rows. |
| `backup/` | 36 pipeline configs from 2020 (mostly `muni-budgets`). Useful as `dgpConfig` examples, but they predate the phase enum (they use `תכנון`). |
| `build_docker.sh` | Pushes `hasadna/datacity-dgp-importer:latest` to Docker Hub. CI doesn't use it. Stale. |
| `generate_key_pair.sh` | Generates an RSA `private.pem`/`public.pem` pair, presumably for the auth (dgp-oauth2) JWT keys. Not referenced by anything in this repo. |
| `setup.py` | Installs nothing (see [architecture.md](architecture.md#how-the-image-is-assembled)). |
| `taxonomies/muni-budgets/header_mapping.yaml` | Never loaded. |
| `muni-budgets` column-type descriptions | `phase` lists values that the enum rejects. `municipality:code` says MOIN, but the value is CBS. |
