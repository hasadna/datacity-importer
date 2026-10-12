# Gotchas

The short list. Details are in the linked docs.

## Deploying and configuration

- **Pushing to `main` deploys to production**: CI bumps `dgpAppImage` in hasadna-k8s. Add
  `--no-deploy` to the commit message to build without deploying.
- **The base image is unpinned** (`akariv/dgp-app:latest`). A rebuild can change dgp-app, dgp or
  Airflow underneath you.
- **`PYTHONPATH=/app` is load-bearing.** `setup.py` installs nothing, so the taxonomies' imports of
  `datacity_server` depend on that path.
- **Editing `column_types.json` doesn't affect existing pipelines.**
  - Runs use the column types frozen in each pipeline's `dgpConfig`.
  - The UI reads them from the `etl_taxonomies` table, which is seeded from the files only once
    (`ON CONFLICT DO NOTHING`).
  - Code changes in `process.py`, `publish.py` and `datacity_server/` do take effect on deploy.
- `taxonomies/muni-budgets/header_mapping.yaml` is dead: `index.yaml` has no `header_mapping` key.

## The DB tables

- **Tables are created once and never altered.** The first publish fixes the columns and the
  primary key. Check `\d <table>` in production rather than inferring from the code.
- **Primary keys only include *mapped* unique fields**, plus the ones `MissingColumnsAdder` adds.
  Fields created by processors (for example `municipality-code` from the name lookup) aren't in
  the key. In a fresh `muni_budgets` table, two municipalities with the same year, card and phase
  collide.

## Runs and re-runs

- **Re-runs are delete-by-`_source`, then insert.** `_source` = `source.path` + `#sheet-<sheet>`.
  - A failed insert leaves that source with **zero rows**.
  - Two pipelines on the same URL and sheet clobber each other.
  - Duplicate keys, in-source or cross-source, fail the run rather than double-counting.
- **Mapping `municipality:code` directly breaks the run.** `MissingColumnsAdder` checks
  `dp.resources[0]`, a leftover reference resource, so it overwrites every taxonomy field with
  `'-'`/`None` and then crashes. Map `municipality:name` only, using the exact lamas spelling, until
  this is fixed.
- **Constraints are enforced only at DB write.** The transform ignores cast errors, so a bad
  `phase` (outside the enum), a non-numeric `value` or an unmatched municipality name gets all the
  way to the dumper and fails the whole run.

## muni-budgets data

- **muni-budgets card codes:**
  - They must start with 10–15 digits. 9-digit codes are padded only when recombined from parts.
  - Dotted codes are dropped.
  - Suffixes (`…-01`) pass the filter and produce garbage economic codes.
  - Codes must be strings.
- `chapters.csv` (9x) and `types.csv` (1/11 EXPENDITURE) contain duplicate keys. Economic names for
  salary codes are wrong.

## Caches, local runs and secrets

- **Caches are relative to the working directory** (`.cache/`, `.checkpoints/`) and are never
  invalidated within a process. Delete them locally after editing reference CSVs or when a source
  changes.
- **Local runs don't behave like production**: 5000-row cap, column types read from the files, no
  publish. Use Postgres, not SQLite, to reproduce DB behaviour.
- **Secrets live untracked in the working copy**: `.env` (production ETLs DB URL),
  `datacity_deploy_key*`, `deploy-key*`, `temp_key*`. Only `.env` is gitignored, so never
  `git add -A`. Never copy their contents anywhere.
