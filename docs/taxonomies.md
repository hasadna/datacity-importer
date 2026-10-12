# Taxonomies

A *taxonomy* is a target schema: a list of **column types** that source columns get mapped to, plus
optional Python modules that enrich and publish the data. Every DataCity pipeline maps one source
(a file, or one sheet of a file) to one taxonomy and publishes it into that taxonomy's DB table.

## How dgp loads `taxonomies/index.yaml`

`dgp.taxonomies.TaxonomyRegistry('taxonomies/index.yaml')` reads one entry per taxonomy id. Each
entry can have these keys:

| Key | Meaning |
|---|---|
| `title` | The display name. All titles here are Hebrew. |
| `column_types` | A JSON file, or a list of files that get concatenated. If the same `name` appears more than once, the entries are **merged**. That's why the duplicate `functional-classification:moin:level1..3:*` entries in `muni-budgets/column_types.json` do no harm. |
| `header_mapping` | A YAML file of known header → column type mappings. **No entry in `index.yaml` sets this key**, so `muni-budgets/header_mapping.yaml` is never loaded. It has been dead since it was added in `61ab14c` (2020). |
| `processing_module` | The taxonomy's `process.py`. It must define `flows(config, context)`, which returns `enrichments_flows(config, context, *EnricherClasses)`. |
| `publishing_module`, `loading_module`, `preloading_module` | These are only set on `_common_`. When a taxonomy doesn't define its own module of a kind, dgp falls back to `_common_`'s. Loading and preloading always use `_common_`. |
| `config` | Free-form taxonomy settings (`missingValues`, …). None of DataCity's taxonomies set it. |

Modules are loaded from their file paths (`spec_from_file_location`), not as packages. That's why
they import shared code from `datacity_server`, which `PYTHONPATH=/app` makes importable.

### Column-type properties

| Property | Effect |
|---|---|
| `name` | The column-type id, like `card:code`. The field name in the output and the DB is the name with `:` → `-`, like `card-code`. |
| `title`, `description` | Shown in the mapper. The `title` and `name` also serve as automatic header matches: a source header that equals either one (case-insensitive) is mapped automatically. |
| `dataType` | The tableschema type the field is cast to: `string`, `integer`, `number`, `date`, `datetime`, `time`, `boolean` or `array`. |
| `unique` | The field becomes part of the **primary key**. See [Primary keys](#primary-keys). |
| `mandatory` | The analysis fails unless the column type is mapped, either to a source column or to a constant. |
| `alternatives` | A list of column-type sets. Mapping all the column types in any one set satisfies `mandatory`. |
| `labelOf` | Marks the human-readable label of a code column. This is metadata only. |
| `options` | Merged into the output field's schema. Examples: `constraints.enum` (muni-budgets `phase`), `format` (dates, `uri`) and `truthValues` (schools `school:active`). Constraints are **enforced at DB-write time**: a row that violates one fails the whole run. |

The workbench also learns header mappings from existing pipelines (dgp-server's
`ConfigHeaderMappings`). A header that has been mapped before in a pipeline of the same taxonomy is
suggested again.

Normalisation ("unpivot") is part of dgp. One wide sheet can carry several years or phases as
separate columns, for example `תקציב 2017`, `תקציב 2018`. Each of those columns gets mapped with
`normalize: {<extra header>: <value>, …}` onto a single target header, which creates one output row
per column. The examples in `muni-budgets/header_mapping.yaml` show the intended use, but they
aren't loaded (see above).

### Primary keys

The primary key is assembled in three places:

1. `PrimaryKeyAnalyzer` (dgp) adds the `unique` column types that are **mapped**, either as source
   columns or constants.
2. `MissingColumnsAdder` (`taxonomies/publish.py`) goes through every column type in the
   taxonomy. If a column type isn't in the data yet, it adds it: `unique` ones get the value `'-'`
   and are **appended to the primary key**; others get `None`. This keeps every run's column set
   the same, which matters because the DB table is never altered (see below).
3. `publish_flow` (dgp-server) re-appends the analyzed key.

Fields created by *processors* aren't added to the key, even when their column type is `unique`.
For example, `municipality-code` filled in by `MunicipalityNameToCodeEnricher`, and
`direction-code` and the `functional-classification-*` codes in muni-budgets, are all left out.
See [muni-budgets.md](muni-budgets.md#primary-key-and-what-it-means-for-loading). This has real
consequences.

`EnsureUniqueFields` (`publish.py`) sets `missingValues: ['']` and replaces `None`/`''` with `'-'`
in every unique field whose type is **string**. Unique *integer* fields don't get this treatment,
so a null there fails the insert, because Postgres requires primary-key columns to be NOT NULL.

## The `_common_` modules

### `taxonomies/loaders.py`: loading analyzers (every taxonomy)

- **`SharepointJson`**: handles SharePoint "GetjsonItems" exports. It runs when `source.format` is
  `json` and no `source.property` is set. It looks for `Root.Items.Item[]` and, if it's there:
  - sets `source.property` to it
  - records each item's `Fields.Field[].Caption` values as extra headers
    (`datacity.extra_headers`) and swaps them in for the `Fields` header
  - at flow time, adds those string fields and unfurls `Fields.Field[]` (`Caption` → `Value`) into
    the row

  It uses tabulator's private attributes (`stream._Stream__source` / `__loader`), so it's fragile
  across tabulator versions.
- **`HTMLTableURLExtractor`**: runs for `source.format == 'html'`.
  - It sets `source.raw_html` so cells keep their HTML.
  - Headers that contain HTML are reduced to their text.
  - It adds an `extracted_url` header and field.
  - At flow time, each HTML cell is reduced to its text. The `href` of the first `<a>` in the row
    (made absolute against `source.path`) goes into `extracted_url`.

  `field_map` is a class attribute, so it's shared between instances in the same process.

### `taxonomies/publish.py`: publishing (every taxonomy)

At module import it runs `create_engine(os.environ['DATASETS_DATABASE_URL'])`, so that variable must
be set even for analysis. It defines three enrichers, all of which always run:

1. **`MissingColumnsAdder`**: see [Primary keys](#primary-keys).
   ⚠️ To decide whether a field already exists, it looks at `dp.resources[0]`, which isn't
   necessarily the `out` resource. Each enricher's pre-flow loads its reference datapackage as an
   extra resource at the *front* of the package, and only an enricher that actually runs consumes
   (joins and deletes) its resource. When one is skipped, its reference resource stays at index 0.
   Then every taxonomy field looks "missing" and gets **overwritten** with `'-'`/`None`. The run
   then dies on the first integer `'-'`. This happens whenever `MunicipalityNameToCodeEnricher` is
   skipped because `municipality:code` is mapped directly. It was reproduced locally (see
   [muni-budgets.md](muni-budgets.md#municipality-mapping-municipalitycode-directly-is-currently-broken)).
   The fix would be to look the resource up by name (`RESOURCE_NAME`, `'out'`) instead of using
   index 0.
2. **`EnsureUniqueFields`**: see above.
3. **`DBWriter`** runs `publish_flow(config, engine, mode='append', fast=True)` from
   `dgp_server.publish_flow`. That flow:
   - adds a `_source` column, set to `source.path`, plus `#sheet-<source.sheet>` when a sheet is
     configured
   - appends the primary key to the schema
   - `clear_by_source`: creates the index `<table>__s` on `_source` (this fails harmlessly after
     the first time and is logged as an error), then runs
     `DELETE FROM <table> WHERE _source = <source>`
   - `dump_to_sql(mode='append', batch_size=1000)` into table `<taxonomy id, - → _>`

## DB tables and re-runs

Each taxonomy gets one table in `DATASETS_DATABASE_URL`, named after the taxonomy id with `-` → `_`:

| Taxonomy id | Table |
|---|---|
| `muni-budgets` | `muni_budgets` |
| `muni-procurement` | `muni_procurement` |
| `muni-businesses` | `muni_businesses` |
| `schools` | `schools` |
| `events` | `events` |
| `expenses` | `expenses` |
| `tax-code-rates` | `tax_code_rates` |
| `property-tax-business-kinds` | `property_tax_business_kinds` |
| `business-licensing-items-by-property` | `business_licensing_items_by_property` |
| `business-kind-common-names` | `business_kind_common_names` |

The `business_licensing_law` operator also writes a `business_licensing_law` table. See
[operators-and-processors.md](operators-and-processors.md).

**Columns.** A table has:
- every mapped column type (`:` → `-`); unmapped source columns are dropped by the transform
- every column type that `MissingColumnsAdder` adds
- fields that processors add (for example `economic-classification-moin-level*-code/name` in
  muni-budgets, which aren't in its `column_types.json`, or `business-kind-order`)
- `_source`

Array and object fields become `jsonb` in Postgres.

**Table creation is a one-time event.** `dump_to_sql` creates the table, with a real `PRIMARY KEY`
constraint, on the first publish ever. It uses *that* run's schema and key. Later runs only
describe the table. They never `ALTER` it: no new columns, no new key and no type changes. To find
out what the production key actually is, run `\d <table>`. Don't infer it from the column types.

**Re-runs replace rows by source, not by key:**
- Each run first deletes every row whose `_source` equals its own `source.path` (+ `#sheet-…`).
  It then inserts all rows in one transaction.
- The delete is committed on its own. If the insert then fails (a duplicate key, a constraint
  violation or a cast error), **that source ends up with zero rows** until a run succeeds. This
  was reproduced locally.
- Two pipelines with the same source URL and sheet delete each other's rows. The last one to run
  wins.
- Rows with the same primary key, whether inside one source or across sources, aren't merged. The
  insert fails with `UniqueViolation` and the run fails. Only the taxonomies that run
  `StreamingDuplicateRemover` remove in-source duplicates first. muni-budgets doesn't.

## The taxonomies

The tables below list unique (primary-key) column types (**U**), mandatory ones (**M**), and the
processing chain in order. Every taxonomy also gets `loaders.py` and `publish.py`. "name→code"
means `MunicipalityNameToCodeEnricher`. `FilterEmptyCodes` means a per-taxonomy subclass of
`FilterEmptyFields`, which drops rows whose listed fields are empty or fail a validator.

### `muni-budgets`: תקציב

Municipal budget lines. → **[muni-budgets.md](muni-budgets.md)**

- **U:** `municipality:code`, `city:code`, `card:code`, `fiscal-year`, `phase`, `direction:code`,
  `functional-classification:moin:level1..4:code`
- **M:** `municipality:name`; `card:code` (or `card:code:part1+2[+3]`); `fiscal-year`; `phase`;
  `value` (or `value-thousands`)

### `muni-procurement`: תהליכי רכש/השמה

Municipal tenders and procurement processes.

- **U:** `municipality:code`, `city:code`, `process:code`, `process:kind`, `process:title`,
  `process:procurer:unit:code`
- **M:** `municipality:name`, `process:kind`, `process:title`, `process:status`
- Other fields: description, decision, publication/closing/start/end/last-update dates
  (`last-update-date` has the format `%d/%m/%Y %H:%M:%S`), awardee name/id, value, procurer
  unit/contact, document cost, purchase link (`uri`), documents (`array`), source link.
- Processing: `FilterEmptyCodes` (`process-title` must be a non-blank string) → name→code.
  `SelectLatestProcessEnricher` (dedupe by publication date) is defined but commented out.

### `muni-businesses`: מאגר עסקים

Businesses in the municipality, usually from property-tax (arnona) data.

- **U:** `municipality:code`, `city:code`, `business:name`, `property:code`, `source:kind`,
  `source:date`
- **M:** `municipality:name`, `business:name`, `property:code`, `source:kind`, `source:date`
- Other fields: corporation id, address (`full`/`city`/`street`/`house-number`), `location:lat/lon`,
  phone, `business:kind`. The Hebrew titles of `location:lat`/`lon` are swapped ("קו אורך" for lat).
- Processing: name→code → `FilterEmptyCodes` (`business-kind`, `address-full`, `business-name`,
  `property-code`) → `StreamingDuplicateRemover` → `AddressFixer` → `GeoCoder`.

### `schools`: מוסדות חינוך

Educational institutions.

- **U:** `school:symbol` only. `municipality:code` *isn't* unique here.
- **M:** `municipality:name`, `school:symbol`
- Other fields: name, address, WGS84 and Israeli-grid coordinates, contact details, kind, phase,
  grades, sector, oversight, legal status, ownership, founding year, school year, student and
  classroom counts, budget, and `school:active` (boolean, `truthValues: ['פעיל']`).
- Processing: `FilterEmptyCodes` (`school-symbol`) → `AddressFixer` → `GeoCoder` → `GeoProjection`.
  There's no name→code step, so `municipality-code` stays empty unless it's mapped.

### `events`: אירועים

Municipal events.

- **U:** `municipality:code`, `city:code`, `event:code`, `event:kind`
- **M:** `municipality:name`, `event:kind`, `event:title`
- Processing: `FilterEmptyCodes` (`event-title` must be a non-blank string) → name→code.

### `expenses`: הוצאות

Payments and expenses. This one has no municipality fields at all.

- **U:** `payer:title`, `payee:title`, `process:code`, `process:kind`, `process:value-kind`,
  `process:approval-year`, `process:procurer:unit:name`, `process:procurement:id`
- **M:** `payer:title`, `payee:title`, `process:kind`, `process:title`, `process:value`
- Dates use `auto_ymd`.
- Processing: `FilterEmptyCodes` (`process-title`, `payee-title` must be non-blank strings).
  name→code is commented out.

### `tax-code-rates`: צו ארנונה

Property-tax rate tables (`property-tax/rates/`).

- **U:** `municipality:code`, `city:code`, `property:tax-code:id`, `…:zone-kind`, `…:zone-id`,
  `…:min-area`, `…:max-area`
- **M:** `municipality:name`, `property:tax-code:id`, `property:tax-code:rate`
- Processing: name→code → `FilterEmptyCodes` (`property-tax-code-id`) →
  `StreamingDuplicateRemover` → `DefaultFiller`. `DefaultFiller` replaces `None` with: zone-kind and
  zone-id `''` (which `EnsureUniqueFields` later turns into `'-'`), `min-area` 0, `max-area` `1e9`.

### `property-tax-business-kinds`: ארנונה עסקית

Each property's tax code and business kind (`property-tax/business-kinds/`).

- **U:** `municipality:code`, `city:code`, `property:code`, `property:tax-code:id`
- **M:** `municipality:name`, `property:code`, `property:tax-code:id`, `business:kind`
- Processing: name→code → `FilterEmptyCodes` (`property-code`, `property-tax-code-id`,
  `business-kind`) → `StreamingDuplicateRemover`.

### `business-licensing-items-by-property`: פריטי רישוי לפי נכס פיזי

Business-licensing item per property (`business-licensing-items/by-property/`).

- **U:** `municipality:code`, `city:code`, `property:code`
- **M:** `municipality:name`, `property:code`, `business-licensing-item:id`
- Processing: name→code → `FilterEmptyCodes` (`property-code`) → `LicensingItemCodeFixer` →
  `StreamingDuplicateRemover`.
  - `LicensingItemCodeFixer` left-pads the item id to 8 digits and splits it into 2-digit integer
    parts joined by `.`. For example `6020100` → `06020100` → `6.2.1.0`. It prints every code to
    stderr.
  - The licensing item isn't part of the key, so a property with several items keeps only the
    first.

### `business-kind-common-names`: שמות נפוצים לסוגי בתי עסק

Maps a municipality's internal business-kind names to common names and licensing items
(`muni-businesses/common-names/`).

- **U:** `municipality:code`, `city:code`, `business:kind:internal`
- **M:** `municipality:name`, `business:kind:internal`, `business:kind`,
  `business-licensing-item:id`, `business:kind:store`
- Processing: name→code → `FilterEmptyCodes` (`business-kind`, `business-kind-internal`) →
  `Enumerator` → `LicensingItemProcessor`.
  - `Enumerator` adds `business-kind-order` with the row index.
  - `LicensingItemProcessor` turns `business-licensing-item-id` from a comma-separated string into
    an **array** (a `jsonb` column, despite `dataType: string`), and appends `6.2` when
    `business-kind-store` is true.
- The `muni_business_kinds` operator reads this table.
