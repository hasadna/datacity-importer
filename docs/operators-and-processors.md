# Operators and processors

## Operators (`operators/` → `/app/dags/operators/`)

An operator is a module with `operator(*args)`. `dags/dag_fetcher.py` calls it as
`operator(name, params, pipeline)`, trimmed to the function's signature. The pipeline's `kind`
selects the module. Both DataCity operators are `admin: true` kinds in `configuration.json`, so
only admins can create them. Neither one uses dgp or the taxonomies.

### `business_licensing_law`

This operator scrapes the Business Licensing Order (צו רישוי עסקים) and writes the table
`business_licensing_law` to `DATASETS_DATABASE_URL`. It uses `dump_to_sql` in its default
**rewrite** mode, so each run replaces the whole table.

- Source 1: `https://www.nevo.co.il/law_html/law01/500_849.htm`, the `table[border="1"]`
  HTML table. Columns: `id`, `itemname`, `environment`, `police`, `ordinance`, `agriculture`,
  `sanitation`, `fire-dept`, `track`, `spec`, `duration`, `notes`.
- Source 2: `https://www.gov.il/he/api/DynamicCollector` (the uniform-specification links,
  paginated). Cached in the `uniform-specs` checkpoint.
- Processing:
  - The authority columns (`+` → `yes`, `[+]` → `attn`, empty → `no`) are normalised after
    stripping a list of known qualifier phrases.
  - `fire-dept` area thresholds are parsed into `firedpt_track` rules (`deposition`/`inspection`).
  - `canonize_id` turns `4.7 א` into `4.7.1` and keeps the original in `orig_id`.
  - `track` text is parsed into area-based rules (`a`/`b`/`deposition`/`full`).
  - `rules()` expands each item into one row per area threshold. Items `6.2` and `7.5` get a
    minimum area of 800.
  - `spec` and `spec_link` come from the gov.il collection.
- The `uniform-specs` and `law` checkpoints are cached in `.checkpoints/` and aren't invalidated. In a
  long-running container, a re-run reuses the cached scrape.
- Running it as a script (`python -m …` / `__main__`) prints items 6.2, 7.5 and 4.7.1.

### `muni_business_kinds`

This operator builds "business kind rules" datasets and publishes them to **CKAN**. It needs
`CKAN_HOST` and `CKAN_API_KEY`, and `dataflows-ckan` from `requirements.dev.txt`.

- `MUNICIPALITIES` is hard-coded to `[('באר שבע', 'beer-sheva')]`.
- It reads these tables from `DATASETS_DATABASE_URL`:
  - `business_kind_common_names`
  - `business_licensing_law`, so `business_licensing_law` must have run first
  - `property_tax_business_kinds`
  - `tax_code_rates`
- The licensing part works per common business kind. It gathers the licensing item ids (a `jsonb`
  array), looks up each item's rules in the law table (falling back to the parent item `id[:-2]`),
  and combines them per area threshold. For each authority it picks the strictest value
  (`yes` > `attn` > `no`; track `full` > `b` > `a` > `deposition`). It takes the minimum duration,
  plus the spec links and item ids.
- The property-tax part works per business kind. It picks the most common zone kind, and then for
  each zone the most common tax code. From that code's area brackets it builds piecewise-linear
  `a·area + b` rules.
- Output: CKAN dataset `<slug>-business-kind-rules` in org `datacity`, with JSON and CSV
  resources. Intermediate datapackages are written under `out/` relative to the working directory.
  The repo's untracked `out/` is left over from local runs.
- The SQL is built with f-strings around the municipality name. That's fine for the hard-coded
  list, but don't feed it user input.

## Processors (`datacity_server/processors`)

These are shared enricher classes imported by the taxonomies' `process.py`. Every one is a dgp
`BaseEnricher`, and they act on the `out` resource unless noted.

### `__init__.py`

- **`MunicipalityNameToCodeEnricher`** (a `DatapackageJoiner`):
  `municipality-name` → lamas `name_municipality` → `symbol_municipality_2015` →
  `municipality-code`.
  - It needs `municipality:name` and is skipped if `municipality:code` is present.
  - The reference is `https://next.obudget.org/datapackages/lamas-municipal-data/datapackage.json`.
    It's downloaded in `prepare()` (when the enricher is constructed, even if it then doesn't
    run) into `.checkpoints/` and `.cache/`.
  - The match is exact. The result is a zero-padded string.
  - See [muni-budgets.md § Municipality](muni-budgets.md#municipality) for the failure modes,
    including the bug when `municipality:code` is mapped.
- **`StreamingDuplicateRemover`**: keeps the first row for each `model.primary_key` tuple, holding
  the seen keys in memory. Bug: for any resource other than `out`, it does `return rows` inside a
  generator, which yields nothing. That empties leftover reference resources. It's harmless,
  because only `out` is written.
- **`FilterEmptyFields`**: the base class for every taxonomy's `FilterEmptyCodes`. Subclasses set
  `FIELDS_TO_CHECK = {field: validator-or-None}`. A row is dropped if a listed field is present and
  falsy, or if its validator returns something falsy. It acts on the *last* resource, which is
  `out`, because reference resources are inserted in front of it.
- **`FillInDefaults`**: `DEFAULT_FIELD_VALUES = {field: default}`. It replaces `None` in the last
  resource. Used by `tax-code-rates`.

### `geo.py`

- **`AddressFixer`**: always runs. If the data has any `address-*` field, it makes sure an
  `address-full` field exists (`columnType: address:full`). It then sets `address-full` to the
  space-joined non-empty values of `address-full`, `-street`, `-house-number` and `-city`, in that
  order. If `address-full` was already mapped, the parts are appended to it.
- **`GeoCoder`** (a `ColumnTypeTester`): needs `address:full`, and is skipped if `location:lat` or
  `location:lon` is present. It adds `location-lat` and `location-lon`.
  - The cache is built from the existing `schools` and `muni_businesses` tables in
    `DATASETS_DATABASE_URL`, keyed by `address-full`, plus an ITM-coordinates cache keyed by school
    symbol, downloaded from the Al-Qasum `Schools.xlsx` on DigitalOcean Spaces.
  - Address preparation strips a leading `שד`, `רח` or `רחוב`.
  - It geocodes up to **1000 new addresses per run**. It uses Google when `GOOGLE_MAPS_API_KEY` is
    set, and otherwise OSM/Nominatim at `https://geocode.datacity.org.il/` (`language='he'`).
  - It fills `location-lat-ilgrid`/`-lon-ilgrid` from the school cache when `school-symbol` matches.
- **`GeoProjection`** (a `ColumnTypeTester`): needs `location:lat:ilgrid` and `location:lon:ilgrid`,
  and is skipped if WGS84 lat/lon are present. It converts Israeli TM grid coordinates (a custom
  pyproj `tmerc` CRS) to lat/lon. In `schools`, it only runs when `GeoCoder` didn't, meaning there's
  no address.
