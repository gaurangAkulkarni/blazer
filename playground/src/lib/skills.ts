export interface Skill {
  id: string
  name: string
  description: string
  builtIn: boolean
  prompt: string
}

export const BUILT_IN_SKILLS: Skill[] = [
  {
    id: 'blazer-engine',
    name: 'Blazer Engine Expert',
    description: 'Full knowledge of the Blazer DataFrame API — expressions, lazy evaluation, casting, aggregations, and large-file patterns.',
    builtIn: true,
    prompt: `## Blazer Engine — Full API Reference

### Data Loading
\`\`\`js
readCsv("/abs/path/file.csv")          // → DataFrame (eager, all rows in RAM)
readParquet("/abs/path/file.parquet")  // → DataFrame (eager)
scanParquet("/abs/path/file.parquet")  // → LazyFrame (single file, lazy)
scanParquet("/abs/path/folder/")       // → LazyFrame (partitioned Parquet dir, lazy)
writeParquet(df, "/abs/path/out.parquet")
convertCsvToParquet("/in.csv", "/out.parquet")  // convert CSV → Parquet, returns output path
\`\`\`

### DataFrame methods
\`\`\`js
df.height()             // number of rows
df.width()              // number of columns
df.columns()            // string[] of column names
df.getSchema()          // [{name, dtype}] — dtype is "Utf8","Int64","Float64","Boolean","Int32","Float32"
df.toString()           // pretty table string
df.head(n?)             // first n rows (default 5)
df.tail(n?)             // last n rows
df.sort("col", desc?)   // sort by column, second arg true = descending
df.selectColumns(["a","b"])  // pick columns → DataFrame
df.lazy()               // → LazyFrame
df.toJSON()             // JSON string of rows (max 10k)
df.vstack(other)        // append rows from another DataFrame (same schema) → DataFrame
\`\`\`

### LazyFrame methods (chain, then .collect())
\`\`\`js
lf.filter(expr)                       // keep rows where expr is true
lf.select([expr1, expr2, ...])        // project columns/expressions
lf.withColumns([expr1, expr2, ...])   // add/overwrite columns
lf.groupBy([keyExpr, ...], [aggExpr, ...])  // group + aggregate (two-arg form)
lf.sort("col", desc?)                 // sort
lf.limit(n)                           // first n rows
lf.distinct()                         // deduplicate rows
lf.collect()                          // execute → DataFrame
lf.collectStreaming()                 // streaming execution → DataFrame (use for huge data)
lf.explain(optimized?)                // show query plan
lf.sinkParquet("/path.parquet")       // stream to file → row count
lf.sinkCsv("/path.csv")              // stream to CSV → row count
lf.withStreamingBudget(bytes)         // set RAM budget for spill-to-disk
\`\`\`

### Expressions — col() and lit()
\`\`\`js
col("name")              // reference a column
lit(42)                  // Int64 literal
lit(3.14)                // Float64 literal
lit("text")              // Utf8 literal
lit(true)                // Boolean literal

// Arithmetic
col("a").add(col("b"))   col("a").sub(col("b"))
col("a").mul(col("b"))   col("a").div(col("b"))
col("a").mul(lit(1.1))

// Comparisons → Boolean expr
col("age").gt(lit(18))   col("age").lt(lit(65))
col("age").gtEq(lit(18)) col("age").ltEq(lit(65))
col("name").eq(lit("Alice"))  col("name").neq(lit("Bob"))

// Logical
expr1.and(expr2)  expr1.or(expr2)  expr.not()

// Null checks
col("x").isNull()  col("x").isNotNull()

// Type casting — ALWAYS needed for CSV numeric columns (dtype Utf8)
col("sales").cast("Float64")   // Utf8 → Float64
col("qty").cast("Int64")       // Utf8 → Int64
col("flag").cast("Boolean")    // "true"/"false" → Boolean
// Accepted: "Float64","Float32","Int64","Int32","Utf8","Boolean"

// Aggregations
col("x").sum()    col("x").mean()   col("x").min()    col("x").max()
col("x").count()  col("x").nUnique()  // count distinct non-null values
col("x").first()  col("x").last()

// Alias
col("sales").sum().alias("total_sales")
\`\`\`

### Auto-injected variables — USE THESE EXACT NAMES
When the user attaches files, these variables are **automatically available in every code block**:
- First attached file → \`df\`
- Second attached file → \`df2\`
- Third → \`df3\`, and so on.

**CRITICAL**: Never declare \`const df = readCsv(...)\` or any custom name for loaded files. Just use \`df\`, \`df2\`, etc. directly — they are pre-loaded in every block. Redefining them with a different name (e.g. \`df_2016_03\`) breaks subsequent code blocks.

### Helpers available in preamble
\`\`\`js
// Combine multiple DataFrames/LazyFrames (same schema) — returns DataFrame
unionAll(df1, df2, df3, ...)
// LazyFrames are collected first; identical to calling df1.vstack(df2).vstack(df3)...

convertCsvToParquet("/in.csv", "/out.parquet")  // returns output path
\`\`\`

### IMPORTANT — methods that do NOT exist (never generate these)
\`\`\`
lf.union(...)       // ← does NOT exist — use unionAll(df1, df2) instead
df.join(...)        // ← does NOT exist
df.filter(...)      // ← does NOT exist on DataFrame — use df.lazy().filter(...).collect()
df.groupBy(...)     // ← does NOT exist on DataFrame — use df.lazy().groupBy(...).collect()
lf.rename(...)      // ← does NOT exist
lf.drop(...)        // ← does NOT exist
col("x").truediv()  // ← does NOT exist — use col("x").div(...)
\`\`\`

### CSV type rules (CRITICAL)
- Every CSV column is **Utf8** unless the schema shows Int64/Float64.
- Always \`cast("Float64")\` before numeric operations: filter, sum, mean, min, max, gt, lt, etc.
- Parquet files are pre-typed — no casting needed.

### Large file patterns
\`\`\`js
// ❌ BAD — loads 2GB CSV into RAM
const df = readCsv("/big.csv")

// ✅ GOOD — convert once, then lazy-scan
convertCsvToParquet("/big.csv", "/big.parquet")
scanParquet("/big.parquet").filter(col("x").cast("Float64").gt(lit(100))).collect()

// ✅ GOOD — multiple files in a folder → one lazy scan
scanParquet("/data/folder/")
  .filter(...)
  .groupBy([col("category")], [col("sales").cast("Float64").sum().alias("total")])
  .sort("total", true)
  .collect()
\`\`\`

### Common patterns
\`\`\`js
// Top N by value
df.lazy()
  .withColumns([col("sales").cast("Float64").alias("sales")])
  .groupBy([col("category")], [col("sales").sum().alias("total")])
  .sort("total", true)
  .limit(10)
  .collect()

// Filter then aggregate
df.lazy()
  .filter(col("region").eq(lit("West")))
  .withColumns([col("profit").cast("Float64").alias("profit")])
  .groupBy([col("sub_category")], [
    col("profit").sum().alias("total_profit"),
    col("order_id").count().alias("orders")
  ])
  .sort("total_profit", true)
  .collect()

// Computed column
df.lazy()
  .withColumns([
    col("profit").cast("Float64").div(col("sales").cast("Float64")).alias("margin")
  ])
  .collect()
\`\`\``,
  },
  {
    id: 'data-analyst',
    name: 'Senior Data Analyst',
    description: 'Thinks like a senior analyst — always checks schema first, explains findings, suggests follow-up questions.',
    builtIn: true,
    prompt: `## Data Analyst Persona
You think like a senior data analyst:
1. Always examine schema/columns before writing queries — never guess names.
2. After showing results, briefly interpret the numbers (e.g. "Technology drives 38% of revenue").
3. Proactively suggest 2–3 follow-up analyses the user might find valuable.
4. Flag data quality issues (nulls, outliers, unexpected values) when spotted.
5. Keep result sets small and meaningful — avoid dumping thousands of rows.`,
  },
]

export function resolveSkillPrompts(activeSkillIds: string[], customSkills: Skill[] = []): string {
  const all = [...BUILT_IN_SKILLS, ...customSkills]
  const active = all.filter((s) => activeSkillIds.includes(s.id))
  if (active.length === 0) return ''
  return active.map((s) => s.prompt).join('\n\n')
}
