# blazer-py

A fast, Rust-powered DataFrame engine for Python.

## Install

```bash
pip install blazer-py
```

## Quick start

```python
import blazer

# Build a DataFrame from Python dicts
df = blazer.DataFrame({
    "product": ["apple", "banana", "cherry"],
    "qty":     [10,      5,        8],
    "price":   [1.2,     0.5,      2.0],
})
print(df)

# Lazy API: filter → derive column → collect
result = (
    df.lazy()
      .filter(blazer.col("qty") > 6)
      .with_columns([(blazer.col("qty") * blazer.col("price")).alias("revenue")])
      .collect()
)
print(result)

# Read Parquet
df = blazer.read_parquet("/data/sales.parquet")

result = (
    blazer.scan_parquet("/data/sales.parquet")
        .filter(blazer.col("amount") > 100)
        .group_by(
            [blazer.col("region")],
            [blazer.col("amount").sum().alias("total")],
        )
        .sort("total", descending=True)
        .collect()
)
print(result)
```

## API overview

| Function / class | Description |
|---|---|
| `blazer.DataFrame(data)` | In-memory columnar table from a `dict[str, list]` |
| `blazer.read_parquet(path)` | Read a Parquet file into a DataFrame |
| `blazer.read_csv(path)` | Read a CSV file into a DataFrame |
| `blazer.scan_parquet(path)` | Lazy scan of a Parquet file / directory |
| `blazer.scan_csv(path)` | Lazy scan of a CSV file |
| `blazer.col(name)` | Reference a column by name |
| `blazer.lit(value)` | Wrap a Python scalar as a literal |

### DataFrame

`height()` · `width()` · `columns()` · `schema()` · `head(n)` · `tail(n)` ·
`sort(by, descending)` · `select_columns(names)` · `to_dict()` · `vstack(other)` ·
`lazy()` · `write_parquet(path)` · `write_csv(path)`

### LazyFrame

`filter(pred)` · `select(exprs)` · `with_columns(exprs)` · `group_by(keys, aggs)` ·
`sort(by, descending)` · `limit(n)` · `distinct()` · `join(other, left_on, right_on, how)` ·
`collect()` · `collect_streaming()` · `sink_parquet(path)` · `sink_csv(path)` ·
`with_streaming_budget(bytes)` · `explain(optimized)` · `explain_streaming()`

### Expr

Aggregations: `sum()` `mean()` `min()` `max()` `count()` `n_unique()` `first()` `last()`

Operators: `>` `<` `==` `!=` `<=` `>=` `+` `-` `*` `/` `&` `|` `~`

Other: `alias(name)` · `cast(dtype)` · `is_null()` · `is_not_null()` ·
`over(partition_by)` · `rolling_mean(window)` ·
`str_contains(p)` · `str_starts_with(p)` · `str_ends_with(p)` ·
`str_to_uppercase()` · `str_to_lowercase()`

## License

MIT
