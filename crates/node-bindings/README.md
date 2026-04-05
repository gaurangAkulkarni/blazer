# blazer-node

A fast, Rust-powered DataFrame engine for Node.js — built on [Apache Arrow](https://arrow.apache.org/) with lazy evaluation, Parquet I/O, and query optimization.

## Install

```bash
npm install blazer-node
```

## Quick start

```js
import { col, lit, readParquet, scanParquet } from 'blazer-node'

// Read a Parquet file eagerly
const df = readParquet('/data/sales.parquet')
console.log(df.toString())

// Lazy API — nothing runs until .collect()
const result = scanParquet('/data/sales.parquet')
  .filter(col('amount').gt(lit(100)))
  .groupBy(
    [col('region')],
    [col('amount').sum().alias('total')]
  )
  .sort('total', true)   // descending
  .collect()

console.log(result.toJSON())
```

## API

### Top-level functions

| Function | Returns | Description |
|---|---|---|
| `col(name)` | `Expr` | Reference a column |
| `lit(value)` | `Expr` | Scalar literal (int/float/str/bool dispatch) |
| `litInt(n)` / `litFloat(n)` / `litStr(s)` / `litBool(b)` | `Expr` | Typed literals |
| `readParquet(path)` | `DataFrame` | Read Parquet into memory |
| `readCsv(path)` | `DataFrame` | Read CSV into memory |
| `scanParquet(path)` | `LazyFrame` | Lazy Parquet scan |
| `scanCsv(path)` | `LazyFrame` | Lazy CSV scan |
| `writeParquet(df, path)` | `void` | Write DataFrame to Parquet |

### `Expr`

**Aggregations**: `sum()` `mean()` `min()` `max()` `count()` `nUnique()` `first()` `last()`

**Arithmetic**: `add(e)` `sub(e)` `mul(e)` `div(e)`

**Comparison**: `gt(e)` `lt(e)` `eq(e)` `neq(e)` `gtEq(e)` `ltEq(e)`

**Logical**: `and(e)` `or(e)` `not()`

**Other**: `alias(name)` · `cast(dtype)` · `isNull()` · `isNotNull()` ·
`over([...partitionBy])` · `rollingMean(windowSize)` ·
`strContains(p)` · `strStartsWith(p)` · `strEndsWith(p)` ·
`strToUppercase()` · `strToLowercase()`

### `LazyFrame`

`filter(pred)` · `select(exprs[])` · `withColumns(exprs[])` ·
`groupBy(keys[], aggs[])` · `sort(by, descending?)` · `limit(n)` · `distinct()` ·
`join(other, leftOn[], rightOn[], how?)` · `collect()` · `collectStreaming()` ·
`sinkParquet(path)` · `sinkCsv(path)` · `withStreamingBudget(bytes)` ·
`explain(optimized?)` · `explainStreaming()`

### `DataFrame`

`height()` · `width()` · `columns()` · `getSchema()` · `head(n?)` · `tail(n?)` ·
`sort(by, descending?)` · `selectColumns(names[])` · `vstack(other)` ·
`toJSON()` · `writeParquet(path)` · `writeCsv(path)` · `lazy()`

## License

MIT
