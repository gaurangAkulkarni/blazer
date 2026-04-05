"""
blazer — Rust-powered DataFrame engine for Python.

    import blazer

    df = blazer.read_parquet("/data/sales.parquet")
    result = (
        df.lazy()
          .filter(blazer.col("amount") > 100)
          .group_by(
              [blazer.col("region")],
              [blazer.col("amount").sum().alias("total")],
          )
          .sort("total", descending=True)
          .collect()
    )
"""

from blazer._blazer import (  # noqa: F401
    DataFrame,
    LazyFrame,
    Expr,
    col,
    lit,
    read_parquet,
    scan_parquet,
    read_csv,
    scan_csv,
)

__all__ = [
    "DataFrame",
    "LazyFrame",
    "Expr",
    "col",
    "lit",
    "read_parquet",
    "scan_parquet",
    "read_csv",
    "scan_csv",
]
