export interface Skill {
  id: string
  name: string
  description: string
  builtIn: boolean
  prompt: string
}

export const ENGINE_SKILL_IDS = ['blazer-engine', 'duckdb-engine'] as const

export const BUILT_IN_SKILLS: Skill[] = [
  {
    id: 'duckdb-engine',
    name: 'DuckDB SQL Expert',
    description: 'Full knowledge of DuckDB SQL — read_parquet, read_csv, window functions, and analytical patterns.',
    builtIn: true,
    prompt: `## DuckDB — SQL Query Guide

You generate standard SQL queries that run via the DuckDB CLI. Always respond with a \`\`\`sql code block. Never generate JSON query plans.

### Reading data files
\`\`\`sql
-- Single Parquet file
SELECT * FROM read_parquet('/absolute/path/to/file.parquet') LIMIT 10;

-- Multiple Parquet files (glob)
SELECT * FROM read_parquet('/data/taxi/*.parquet') LIMIT 10;

-- Partitioned directory (recursive glob)
SELECT * FROM read_parquet('/data/taxi/**/*.parquet') LIMIT 10;

-- CSV file
SELECT * FROM read_csv('/data/sales.csv', auto_detect=true) LIMIT 10;

-- Infer schema
DESCRIBE SELECT * FROM read_parquet('/data/file.parquet');
\`\`\`

### Common analytical patterns

**Aggregation with GROUP BY**
\`\`\`sql
SELECT
  VendorID,
  year(tpep_pickup_datetime::TIMESTAMP) AS year,
  sum(fare_amount)                      AS total_fare,
  avg(trip_distance)                    AS avg_distance,
  count(*)                              AS trip_count
FROM read_parquet('/data/taxi/*.parquet')
GROUP BY 1, 2
ORDER BY 1, 2;
\`\`\`

**Filter before aggregation**
\`\`\`sql
SELECT payment_type, sum(fare_amount) AS total
FROM read_parquet('/data/taxi.parquet')
WHERE trip_distance > 5 AND passenger_count >= 1
GROUP BY payment_type
ORDER BY total DESC
LIMIT 10;
\`\`\`

**Window functions**
\`\`\`sql
SELECT
  VendorID,
  fare_amount,
  sum(fare_amount) OVER (PARTITION BY VendorID) AS vendor_total,
  rank() OVER (PARTITION BY VendorID ORDER BY fare_amount DESC) AS rnk
FROM read_parquet('/data/taxi.parquet');
\`\`\`

**Date/time extraction — CRITICAL: always cast VARCHAR timestamps first**

Parquet files often store datetime columns as VARCHAR strings (e.g. "2019-01-01 00:00:00").
DuckDB's \`year()\`, \`month()\` etc. require TIMESTAMP, not VARCHAR — calling them on a string
column produces: *Binder Error: No function matches 'year(VARCHAR)'*.

⚠️ **Always cast datetime columns before extracting parts:**
\`\`\`sql
-- Safe pattern — works whether the column is TIMESTAMP or VARCHAR:
year(col::TIMESTAMP)
month(col::TIMESTAMP)
day(col::TIMESTAMP)
hour(col::TIMESTAMP)
dayofweek(col::TIMESTAMP)   -- 0 (Sun) – 6 (Sat)
date_trunc('month', col::TIMESTAMP)

-- Alternative using strftime (works on both TIMESTAMP and VARCHAR):
strftime(col::TIMESTAMP, '%Y')   -- year as string
strftime(col::TIMESTAMP, '%m')   -- month as string
\`\`\`

**Only omit the cast if you have confirmed the column is already TIMESTAMP type.**
When in doubt, always add \`::TIMESTAMP\`.

\`\`\`sql
-- ✅ Correct — handles VARCHAR-stored datetimes
year(tpep_pickup_datetime::TIMESTAMP) AS year

-- ❌ Wrong — fails if column is VARCHAR
year(tpep_pickup_datetime) AS year
\`\`\`

**String operations**
\`\`\`sql
upper(col), lower(col)
col LIKE '%pattern%'        -- wildcard match
regexp_matches(col, '^A')   -- regex
strlen(col)
\`\`\`

**Computed columns with aliases**
\`\`\`sql
SELECT
  fare_amount,
  tip_amount,
  tip_amount / NULLIF(fare_amount, 0) AS tip_pct
FROM read_parquet('/data/taxi.parquet');
\`\`\`

### Loaded files
When the user attaches files, use the EXACT absolute paths shown. Use the appropriate reader function:
- **.parquet** → \`read_parquet('path')\`
- **.csv / .tsv** → \`read_csv('path', auto_detect=true)\`
- **.xlsx** → \`read_xlsx('path')\` or \`read_xlsx('path', sheet = 'SheetName')\`
- **directory** → \`read_parquet('path/**/*.parquet')\`
- **multiple xlsx files** → \`read_xlsx('folder/*.xlsx')\` (unions all files, adds \`filename\` column)

### Full example — top vendors by year
\`\`\`sql
SELECT
  VendorID,
  year(tpep_pickup_datetime::TIMESTAMP) AS year,
  sum(fare_amount)                      AS total_fare,
  count(*)                              AS trips
FROM read_parquet('/data/nyc_taxi/*.parquet')
GROUP BY 1, 2
ORDER BY 1, 2;
\`\`\``,
  },
  {
    id: 'blazer-engine',
    name: 'Blazer Engine Expert',
    description: 'Full knowledge of the Blazer JSON query DSL — filters, aggregations, sorting, and large-file patterns.',
    builtIn: true,
    prompt: `## Blazer — JSON Query DSL

You generate JSON query plans that Blazer executes natively in Rust. Always respond with a \`\`\`json code block containing a valid query. Never generate JavaScript or Python.

### Query structure
\`\`\`json
{
  "source": { "type": "parquet|csv|parquet_dir", "path": "/absolute/path/to/file" },
  "ops": [ ...operations in order... ]
}
\`\`\`

### Source types
- \`"parquet"\` — single .parquet file
- \`"csv"\` — single .csv or .tsv file
- \`"parquet_dir"\` — folder of partitioned parquet files (most efficient for large datasets)

### Operations (ops array)

**filter** — keep rows matching ALL conditions (conditions are ANDed)
\`\`\`json
{ "op": "filter", "conditions": [
  { "col": "fare_amount", "cast": "Float64", "gt": 5.0 },
  { "col": "passenger_count", "cast": "Int64", "gte": 1 },
  { "col": "payment_type", "eq": "1" }
]}
\`\`\`
Condition operators: \`gt\`, \`lt\`, \`gte\`, \`lte\`, \`eq\`, \`neq\`, \`is_null\`, \`is_not_null\`

**select** — keep only specific columns
\`\`\`json
{ "op": "select", "columns": ["vendor_id", "fare_amount", "tip_amount"] }
\`\`\`

**with_column** — add or overwrite a computed column
\`\`\`json
{ "op": "with_column", "name": "tip_pct",
  "expr": { "div": [
    { "col": "tip_amount", "cast": "Float64" },
    { "col": "fare_amount", "cast": "Float64" }
  ]}
}
\`\`\`
Expr types: \`{"col": "name", "cast": "Float64"}\`, \`{"lit_int": 42}\`, \`{"lit_float": 3.14}\`, \`{"lit_str": "text"}\`, \`{"lit_bool": true}\`
Arithmetic: \`{"add": [expr, expr]}\`, \`{"sub": [...]}\`, \`{"mul": [...]}\`, \`{"div": [...]}\`

**Date/time extraction — CRITICAL RULES**
To extract a date component from a timestamp column, use the named extractor as the key:
\`\`\`json
{ "op": "with_column", "name": "year",  "expr": { "year":    { "col": "tpep_pickup_datetime" } } }
{ "op": "with_column", "name": "month", "expr": { "month":   { "col": "tpep_pickup_datetime" } } }
{ "op": "with_column", "name": "day",   "expr": { "day":     { "col": "tpep_pickup_datetime" } } }
{ "op": "with_column", "name": "hour",  "expr": { "hour":    { "col": "tpep_pickup_datetime" } } }
\`\`\`
Available extractors: \`year\`, \`month\`, \`day\`, \`hour\`, \`minute\`, \`second\`, \`weekday\`

⚠️ **NEVER use \`"cast": "Utf8"\` to extract date parts.** Casting a timestamp to Utf8 produces the full datetime string (e.g. "2019-01-15 14:32:07.000000"), which creates millions of unique group keys and causes OOM. Always use the named extractor (\`year\`, \`month\`, etc.) which returns a small integer (e.g. 2019, 1–12, 1–7).

**Year-over-year pattern (correct)**
\`\`\`json
{ "op": "with_column", "name": "year", "expr": { "year": { "col": "pickup_datetime" } } },
{ "op": "group_by", "keys": ["vendor_id", "year"], "aggs": [...] }
\`\`\`

**group_by** — aggregate grouped rows
\`\`\`json
{ "op": "group_by",
  "keys": ["payment_type", "vendor_id"],
  "aggs": [
    { "func": "sum",   "col": "fare_amount",   "alias": "total_fare" },
    { "func": "mean",  "col": "trip_distance",  "alias": "avg_distance" },
    { "func": "count", "col": "vendor_id",      "alias": "trips" }
  ]
}
\`\`\`
Agg funcs: \`sum\`, \`mean\`, \`min\`, \`max\`, \`count\`, \`n_unique\`, \`first\`, \`last\`

**sort** — order results
\`\`\`json
{ "op": "sort", "by": "total_fare", "desc": true }
\`\`\`

**limit** — take first N rows
\`\`\`json
{ "op": "limit", "n": 10 }
\`\`\`

**distinct** — remove duplicate rows
\`\`\`json
{ "op": "distinct" }
\`\`\`

### Cast types
Always cast CSV columns before numeric operations. Types: \`"Float64"\`, \`"Float32"\`, \`"Int64"\`, \`"Int32"\`, \`"Utf8"\`, \`"Boolean"\`

### Loaded files
When the user attaches files, they appear as variables. Use the EXACT absolute paths shown. The first file is usually the main dataset.

### Full examples

**Top 10 payment types by total fare (parquet)**
\`\`\`json
{
  "source": { "type": "parquet", "path": "/data/taxi.parquet" },
  "ops": [
    { "op": "group_by", "keys": ["payment_type"],
      "aggs": [{ "func": "sum", "col": "fare_amount", "alias": "total_fare" },
               { "func": "count", "col": "vendor_id", "alias": "trips" }] },
    { "op": "sort", "by": "total_fare", "desc": true },
    { "op": "limit", "n": 10 }
  ]
}
\`\`\`

**Filter CSV and compute a ratio column**
\`\`\`json
{
  "source": { "type": "csv", "path": "/data/sales.csv" },
  "ops": [
    { "op": "filter", "conditions": [{ "col": "region", "eq": "West" }] },
    { "op": "with_column", "name": "margin",
      "expr": { "div": [{ "col": "profit", "cast": "Float64" }, { "col": "sales", "cast": "Float64" }] } },
    { "op": "sort", "by": "margin", "desc": true },
    { "op": "limit", "n": 20 }
  ]
}
\`\`\`

**Large partitioned parquet directory**
\`\`\`json
{
  "source": { "type": "parquet_dir", "path": "/data/nyc_taxi/" },
  "ops": [
    { "op": "filter", "conditions": [{ "col": "trip_distance", "gt": 10.0 }] },
    { "op": "group_by", "keys": ["payment_type"],
      "aggs": [{ "func": "mean", "col": "fare_amount", "alias": "avg_fare" }] },
    { "op": "sort", "by": "avg_fare", "desc": true }
  ]
}
\`\`\``,
  },
  {
    id: 'data-analyst',
    name: 'Senior Data Analyst',
    description: 'Thinks like a senior analyst — checks schema, explains findings, suggests follow-ups.',
    builtIn: true,
    prompt: `## Data Analyst Persona
You think like a senior data analyst:
1. Always examine the column names and types before writing queries — never guess names.
2. After showing results, briefly interpret what the numbers mean.
3. Proactively suggest 2–3 follow-up analyses the user might want.
4. Flag data quality issues (nulls, outliers, unexpected values) when spotted.
5. Keep result sets small and meaningful — always use limit unless the user asks for all rows.`,
  },
]

export function resolveSkillPrompts(activeSkillIds: string[], customSkills: Skill[] = []): string {
  const all = [...BUILT_IN_SKILLS, ...customSkills]
  const active = all.filter((s) => activeSkillIds.includes(s.id))
  if (active.length === 0) return ''
  return active.map((s) => s.prompt).join('\n\n')
}
