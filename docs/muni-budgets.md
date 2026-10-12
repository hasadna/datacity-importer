# muni-budgets (תקציב)

This taxonomy holds municipal budget lines: one row per municipality × fiscal year × phase ×
budget card (כרטיס תקציבי). It's published to the table `muni_budgets` in
`DATASETS_DATABASE_URL`.

Files:
- `taxonomies/muni-budgets/column_types.json`: the schema
- `taxonomies/muni-budgets/process.py`: the enrichers
- `taxonomies/muni-budgets/chapters/`: the functional classification (פרקים/סעיפים), 680 rows
- `taxonomies/muni-budgets/types/`: the economic classification (סוגים), 167 rows
- `taxonomies/muni-budgets/header_mapping.yaml`: **not loaded**, because `index.yaml` has no
  `header_mapping` key

> **Verified behaviour.** The statements marked *(tested)* come from running the real pipeline
> (dgp 0.1.3, dgp-server 0.0.95, dataflows 0.5.12, the same versions the image installs) against a
> local Postgres 14, on 2026-10-11. The analysis → `publish.allowed=True` → publish sequence
> mirrored the `dgp_kind` operator.

## Column types

| Column type → field | Type | U | M | Notes |
|---|---|---|---|---|
| `municipality:code` → `municipality-code` | integer | ✔ | | The description says "מזהה משרד הפנים", but when filled automatically it's the **CBS** symbol (`symbol_municipality_2015`). See [Municipality](#municipality). |
| `municipality:name` | string | | ✔ | `labelOf` code. |
| `city:code` | string | ✔ | | A locality inside the authority. Usually unmapped, which gives `'-'`. |
| `city:name` | string | | | |
| `card:code` | string | ✔ | ✔ | Alternatives: `[part1, part2]` or `[part1, part2, part3]`. |
| `card:name` | string | | | |
| `card:code:part1..3` | string | | | A card code split across columns. Recombined, then dropped. |
| `fiscal-year` | integer | ✔ | ✔ | |
| `phase` | string | ✔ | ✔ | `enum: [מקורי, מאושר, ביצוע]`. See [Phase](#phase). |
| `direction:code` | string | ✔ | | Derived: `REVENUE` / `EXPENDITURE`. |
| `functional-classification:moin:level1..4:code` | string | ✔ | | Derived from the card code. |
| `functional-classification:moin:level1..4:name` | string | | | Derived from `chapters/`. |
| `functional-classification:moin:level1..4:name-en` | string | | | **Never filled.** `NAME_EN` exists in `chapters.csv` but isn't fetched. |
| `value` | number | | ✔ | NIS. Alternative: `value-thousands`. |
| `value-thousands` | number | | | Thousands of NIS. Converted into `value`. |

Not in `column_types.json` but always added by `process.py`:
`economic-classification-moin-level1..3-code` and `…-name`. They're columns in the table, but not
in the UI's column-type list and not in the primary key.

`column_types.json` lists the level1–3 functional code/name/name-en entries twice. The registry
merges duplicates by name, so this does no harm.

## Processing chain (`process.py`)

These run in order. Each one checks the column types present in the data at that point.
"Skipped if" means the enricher passes the data through unchanged.

1. **`MunicipalityNameToCodeEnricher`**: needs `municipality:name`; skipped if `municipality:code`
   is present. See [Municipality](#municipality).
2. **`HandleThousandsValues`**: needs `value-thousands`; skipped if `value` is present. It parses
   `value-thousands` as a number with `groupChar=','` and `decimalChar='.'`. Unparseable values are
   **cleared to null**, not errors. It sets `value = value-thousands × 1000` and drops the
   `value-thousands` column (`MissingColumnsAdder` re-adds it as empty at publish time).
3. **`RecombineCardCode`**: needs `card:code:part1` and `part2`; skipped if `card:code` is present.
   See [Card codes](#card-codes). Drops the part columns.
4. **`FilterEmptyCodes`**: drops rows whose `card-code` is empty or doesn't match `[0-9]{10,15}`
   **at the start** of the string. It uses `re.match`, not `fullmatch`.
5. **`CardFunctionalCodeSplitter`**: adds functional levels 1–4 (skipped if any of them is already
   mapped).
6. **`CardEconomicCodeSplitter`**: adds economic levels 1–3 (skipped if any of them is already
   present).
7. **`CardCode1ToDirection`**: functional level 1 → `chapters.DIRECTION`, giving `direction-code`.
8. **`CardCode1..4ToOfficialCardName`**: functional level N → `chapters.NAME`.
9. **`CardECode1..3ToOfficialCardName`**: economic level N **and** `direction-code` →
   `types.NAME`.

Steps 7–9 are `DatapackageJoiner`s: a dataflows `join` in half-outer mode, so unmatched rows are
kept with null names. Keys are compared as formatted strings, so the integer `CODE` in the CSV
matches the string codes.

Reference data is loaded once per process working directory and cached under
`.checkpoints/<md5(path)>` and `.cache/<md5(path)>`. If you edit `chapters.csv` or `types.csv`
locally, delete those directories or you'll keep getting the old copy. In production every new
container starts with an empty cache.

## Card codes

A MOIN (משרד הפנים) budget card is treated as `P FFFFFF EEE`:

```
position:  0   1 2 3 4 5 6   7 8 9      (10-digit code)
           P   F F F F f f   E E E
           │   └ functional ┘ └ economic (סוג), always the LAST 3 characters
           └ prefix digit, ignored for classification (padding adds "1")
```

- Functional level *N* = `code[1 : N+1]`, after removing dots. Level 1 is 1 digit (פרק), level 2
  is 2 digits, level 3 is 3 (סעיף), and level 4 is 4. Positions 5–6 aren't classified.
- Economic level *N* = `code[-3:][:N]`. These are taken from the **end** of the string and the
  dots *aren't* removed first.
- For 11–15-digit codes, the functional digits are still read from the start and the economic
  digits from the end. The extra middle digits are ignored.

**What is accepted** *(tested)*:

| Input mapped as `card:code` | Result |
|---|---|
| `1611000110` (10 digits) | Kept. f1=`6`, f4=`6110`, e3=`110`, direction `EXPENDITURE`. |
| `1111000100` | Kept. f1=`1` → `REVENUE`, level-3 name "ארנונה כללית", economic "ארנונות". |
| `16110001109` (11 digits) | Kept. f4=`6110`, e3=`109`. |
| `611000110` (9 digits) | **Dropped.** Padding only happens when the code is recombined from parts. |
| `1.611.000.120` | **Dropped.** The regex runs before the dots are removed, so the dot-stripping in the splitter is effectively dead code. |
| `1611000130-01` | **Kept**, because the regex only checks the prefix. Economic code becomes `-01` and the economic names are null. |

**Recombining from parts** (`card:code:part1`, `part2`, optional `part3`). The parts are joined in
order, but the loop stops at the first part column that **isn't in the row**. A part column that
*is* there but empty is just skipped. After joining:
- 9 characters → `'1' + cc`
- 6 characters → `'1' + cc + '000'` (a functional-only code)
- any other length → unchanged, and then subject to the 10–15-digit filter

Parts must be strings. With `force_strings` off, an Excel or JSON numeric value isn't cast to a
string (the transform validates with `on_error=ignore`), and `''.join` / `.strip()` would then
raise. The same applies to `card:code` itself.

**Reference data quirks:**
- `chapters.csv` has duplicate `CODE`s: `9` (מפעלים / תקציבי עזר) and `91`–`99`. Each join picks
  one of the duplicates.
- `types.csv` has duplicate `(CODE, DIRECTION)` keys: `(1, EXPENDITURE)` and `(11, EXPENDITURE)`.
  *(tested)*: for card `1611000110`, economic level 1 came out as "העברה (מספר שלילי)" instead of
  "משכורות וש"ע…", and level 2 as "השתתפות בפרקים אחרים" instead of "השכר הקובע". **Economic names
  for salary codes (1xx) are wrong.**

## Direction

`direction-code` comes from functional level 1 (the code's second digit) in `chapters.csv`:
**1–5 → `REVENUE`, 6–9 → `EXPENDITURE`**. If there's no match, the value is null, which
`EnsureUniqueFields` turns into `'-'`. The economic-name joins key on direction, because
`types.csv` reuses codes for revenue and expenditure.

## Values

- Map **`value`** when the amount is already in NIS. It's cast as a plain number: no group
  separators, no parenthesised negatives. A value that doesn't parse passes the transform (errors
  are ignored there) and then **fails the run at DB write**.
- Map **`value-thousands`** (and *not* `value`) when the amount is in thousands of NIS. It's parsed
  leniently (`1,234.5` is fine, garbage becomes null) and multiplied by 1000.

## Phase

`phase` is a primary-key field with `enum: ["מקורי", "מאושר", "ביצוע"]`, enforced at DB write.
Any other value fails the run *(tested: `מתוכנן`)*. The code doesn't define what the phases mean.
The conventional meanings in Israeli municipal budgeting are:

- **מקורי**: the original budget, as the council approved it for the year.
- **מאושר**: the approved budget including in-year amendments (sometimes called "מעודכן").
- **ביצוע**: actual execution (actuals).

The column type's description still says "למשל: מתוכנן, מאושר, מבוצע", which predates the enum
(added in `3c99360`, 2020-07-19). The 2020 pipelines in `backup/` used `תכנון`. Because each
pipeline freezes its column types (see [architecture.md](architecture.md#pipeline-lifecycle)),
pipelines saved before the enum existed don't enforce it.

## Municipality

**`MunicipalityNameToCodeEnricher`** (`datacity_server/processors`) joins `municipality-name`
against `name_municipality` in
`https://next.obudget.org/datapackages/lamas-municipal-data/datapackage.json` (255 authorities)
and fetches `symbol_municipality_2015`.

- The match is **exact string equality**: spacing, hyphens and geresh all have to match, for
  example `תל אביב -יפו`.
- An unmatched name gives `municipality-code = null`, and the run **fails** at DB write: the
  field carries lamas' `required` constraint *(tested: `ירושלם`)*.
- `symbol_municipality_2015` is a zero-padded **string** in lamas (`0070` for Ashdod, `5000` for
  Tel Aviv). When this enricher creates the column, its type is text, *(tested)* the table column
  became `text`. When a pipeline mapped a numeric code directly, its type is integer. Which one the
  production column has depends on which run created the table. Check `\d muni_budgets`.

### Municipality: mapping `municipality:code` directly is currently broken

*(tested)* If a pipeline maps `municipality:code`, whether from a source column or as a constant
(with or without `municipality:name`), the run **wipes and then crashes**:

1. `MunicipalityNameToCodeEnricher` is skipped, but its pre-flow already added the lamas
   reference resource at index 0 of the package.
2. `MissingColumnsAdder` (`publish.py`) checks `dp.resources[0]`, which is that leftover resource.
   It concludes that **every** taxonomy field is missing and overwrites them in `out` with `'-'`
   or `None`: card code, value, name, everything.
3. The DB writer then fails: `Field "municipality-code" can't cast value "-" for type "integer"`.

A fix (not applied): in `MissingColumnsAdder.no_such_field`, look up the resource named
`RESOURCE_NAME` instead of `dp.resources[0]`. The same bug affects every taxonomy that uses
`MunicipalityNameToCodeEnricher` whenever `municipality:code` is mapped.

## Primary key, and what it means for loading

In theory the key is all the `unique` fields: municipality code, city code, card code, fiscal
year, phase, direction, and functional levels 1–4. In practice, only fields that are *mapped*, or
that `MissingColumnsAdder` adds, make it into the key. Fields created by processors don't.
*(tested)* A fresh table created by a normal pipeline (municipality name, year, card code, phase,
value) got:

```
PRIMARY KEY ("fiscal-year", "card-code", phase, "city-code")
```

`municipality-code` isn't part of it. The consequences, all *(tested)*:

- **Two different municipalities** with the same `(fiscal-year, card-code, phase)` collide. The
  second one's run fails with `UniqueViolation`. With 10-digit MOIN codes, collisions across
  municipalities are the norm.
- The production key is whatever the first-ever publish into `muni_budgets` created, and later
  runs never change it. **Check `\d muni_budgets` in production before loading anything.** If it
  lacks `municipality-code`, multi-municipality loading needs a schema fix first (a manual
  `ALTER TABLE … DROP CONSTRAINT / ADD PRIMARY KEY`), and fixing `publish.py` alone won't help.
- A possible stop-gap: `city-code` *is* in the key (`MissingColumnsAdder` adds it, and so does
  mapping it). So mapping `city:code` to the authority code (a constant per pipeline) would keep
  municipalities apart, provided the production key includes `city-code`. It's a hack, so prefer
  fixing the key.

## Re-runs and double counting

- Every run deletes all rows with its own `_source` (`source.path` + `#sheet-<sheet>`), then
  inserts. Re-running the same source replaces its rows *(tested: changed values were replaced,
  not duplicated)*.
- **Identical keys across sources don't double-count. They fail.** The second source's insert
  raises `UniqueViolation` *(tested: same municipality, year, card and phase from a second file)*.
- **Duplicate keys inside one source also fail.** muni-budgets has no dedupe step. Sub-total lines
  that repeat a card code are enough to trigger it.
- **A failed re-run empties that source.** The delete is committed before the insert transaction,
  which then rolls back *(tested: 4 rows → 0)*.
- Two pipelines on the **same URL and sheet** delete each other's rows. For example, one file read
  twice for different phases or years: the last run wins. Use dgp's normalize/unpivot in a single
  pipeline instead, or give each one a distinct URL or sheet.
- **Double counting is still possible** when the same budget reaches the table with *different*
  keys. Examples: one file uses 10-digit codes and another 11-digit; a summary sheet and a detail
  sheet (different `_source`, different cards); or a different `city-code`. Consumers that sum by
  municipality, year and phase will add both.

## Checklist for automated loading (e.g. from budgetkey-data-pipelines)

- Give each pipeline exactly one `(source URL, sheet)`. That's the replace unit.
- Map `municipality:name` with the **exact lamas spelling**. Don't map `municipality:code` until the
  `MissingColumnsAdder` bug is fixed. Confirm the production primary key first (see above).
- Send `card:code` as a **string of 10 digits** (11–15 also work), with no dots, dashes or
  suffixes. Pad 9-digit codes yourself.
- `fiscal-year`: an integer. `phase`: exactly `מקורי`, `מאושר` or `ביצוע`.
- `value`: a plain number in NIS.
- Dedupe `(year, card, phase)` within each source before loading.
- If you create pipelines programmatically, include `taxonomy.columnTypes` in `dgpConfig`. Copy it
  from a pipeline saved by the UI, or run the analysis with `publish.allowed` false first. Runs
  with `publish.allowed=True` never fill in column types: they default to `[]`, which leaves no
  types, no key and no mandatory checks.
