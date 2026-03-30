use std::fmt;

use crate::dtype::DataType;
use crate::error::{BlazeError, Result};
use crate::lazy::LazyFrame;
use crate::schema::{Field, Schema};
use crate::series::Series;

/// A DataFrame is a collection of named, typed, equal-length Series (columns).
#[derive(Clone)]
pub struct DataFrame {
    columns: Vec<Series>,
    schema: Schema,
}

impl DataFrame {
    pub fn new(columns: Vec<Series>) -> Result<Self> {
        if columns.is_empty() {
            return Ok(DataFrame {
                columns: Vec::new(),
                schema: Schema::empty(),
            });
        }
        let expected_len = columns[0].len();
        for col in &columns[1..] {
            if col.len() != expected_len {
                return Err(BlazeError::SchemaMismatch(format!(
                    "Column '{}' has length {}, expected {}",
                    col.name(),
                    col.len(),
                    expected_len
                )));
            }
        }
        let fields: Vec<Field> = columns
            .iter()
            .map(|c| Field::new(c.name(), c.dtype().clone()))
            .collect();
        let schema = Schema::new(fields);
        Ok(DataFrame { columns, schema })
    }

    pub fn empty() -> Self {
        DataFrame {
            columns: Vec::new(),
            schema: Schema::empty(),
        }
    }

    // ---- Accessors ----

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn height(&self) -> usize {
        if self.columns.is_empty() {
            0
        } else {
            self.columns[0].len()
        }
    }

    pub fn width(&self) -> usize {
        self.columns.len()
    }

    pub fn columns(&self) -> &[Series] {
        &self.columns
    }

    pub fn column(&self, name: &str) -> Result<&Series> {
        let idx = self.schema.index_of(name).ok_or_else(|| {
            BlazeError::ColumnNotFound(name.to_string())
        })?;
        Ok(&self.columns[idx])
    }

    pub fn column_by_index(&self, idx: usize) -> Result<&Series> {
        self.columns.get(idx).ok_or_else(|| BlazeError::OutOfBounds {
            index: idx,
            length: self.columns.len(),
        })
    }

    pub fn get_column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name()).collect()
    }

    // ---- Mutations ----

    pub fn with_column(mut self, series: Series) -> Result<Self> {
        let name = series.name().to_string();
        if !self.columns.is_empty() && series.len() != self.height() {
            return Err(BlazeError::SchemaMismatch(format!(
                "Column '{}' has length {}, expected {}",
                name,
                series.len(),
                self.height()
            )));
        }
        if let Some(&idx) = self.schema.index.get(&name) {
            self.columns[idx] = series;
        } else {
            self.columns.push(series);
        }
        // Rebuild schema
        let fields: Vec<Field> = self
            .columns
            .iter()
            .map(|c| Field::new(c.name(), c.dtype().clone()))
            .collect();
        self.schema = Schema::new(fields);
        Ok(self)
    }

    pub fn drop_column(mut self, name: &str) -> Result<Self> {
        let idx = self.schema.index_of(name).ok_or_else(|| {
            BlazeError::ColumnNotFound(name.to_string())
        })?;
        self.columns.remove(idx);
        let fields: Vec<Field> = self
            .columns
            .iter()
            .map(|c| Field::new(c.name(), c.dtype().clone()))
            .collect();
        self.schema = Schema::new(fields);
        Ok(self)
    }

    pub fn select_columns(&self, names: &[&str]) -> Result<Self> {
        let mut cols = Vec::with_capacity(names.len());
        for name in names {
            cols.push(self.column(name)?.clone());
        }
        DataFrame::new(cols)
    }

    pub fn rename_column(mut self, old: &str, new: &str) -> Result<Self> {
        let idx = self.schema.index_of(old).ok_or_else(|| {
            BlazeError::ColumnNotFound(old.to_string())
        })?;
        self.columns[idx].rename(new);
        let fields: Vec<Field> = self
            .columns
            .iter()
            .map(|c| Field::new(c.name(), c.dtype().clone()))
            .collect();
        self.schema = Schema::new(fields);
        Ok(self)
    }

    // ---- Filter ----

    pub fn filter(&self, mask: &arrow2::array::BooleanArray) -> Result<Self> {
        let columns: Result<Vec<Series>> = self
            .columns
            .iter()
            .map(|c| c.filter(mask))
            .collect();
        DataFrame::new(columns?)
    }

    // ---- Sort ----

    pub fn sort(&self, by: &str, descending: bool) -> Result<Self> {
        let col = self.column(by)?;
        let indices = col.argsort(descending)?;
        let columns: Result<Vec<Series>> = self
            .columns
            .iter()
            .map(|c| c.take(&indices))
            .collect();
        DataFrame::new(columns?)
    }

    // ---- Head / Tail ----

    pub fn head(&self, n: usize) -> Self {
        let n = n.min(self.height());
        let columns: Vec<Series> = self.columns.iter().map(|c| c.slice(0, n)).collect();
        DataFrame::new(columns).unwrap_or_else(|_| DataFrame::empty())
    }

    pub fn tail(&self, n: usize) -> Self {
        let h = self.height();
        let n = n.min(h);
        let offset = h - n;
        let columns: Vec<Series> = self.columns.iter().map(|c| c.slice(offset, n)).collect();
        DataFrame::new(columns).unwrap_or_else(|_| DataFrame::empty())
    }

    // ---- Lazy API entry point ----

    pub fn lazy(self) -> LazyFrame {
        LazyFrame::from_dataframe(self)
    }

    // ---- Vertical concat ----

    pub fn vstack(&self, other: &DataFrame) -> Result<Self> {
        if self.width() != other.width() {
            return Err(BlazeError::SchemaMismatch(
                "Cannot vstack DataFrames with different widths".into(),
            ));
        }
        let columns: Result<Vec<Series>> = self
            .columns
            .iter()
            .zip(other.columns.iter())
            .map(|(a, b)| {
                let mut chunks = a.chunks().to_vec();
                chunks.extend(b.chunks().to_vec());
                Series::from_chunks(a.name(), chunks)
            })
            .collect();
        DataFrame::new(columns?)
    }

    // ---- Horizontal concat ----

    pub fn hstack(mut self, columns: Vec<Series>) -> Result<Self> {
        for col in columns {
            self = self.with_column(col)?;
        }
        Ok(self)
    }
}

impl fmt::Display for DataFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "shape: ({}, {})", self.height(), self.width())?;

        // Header
        let names: Vec<&str> = self.columns.iter().map(|c| c.name()).collect();
        let dtypes: Vec<String> = self.columns.iter().map(|c| format!("{}", c.dtype())).collect();

        // Column widths
        let widths: Vec<usize> = names
            .iter()
            .zip(dtypes.iter())
            .map(|(n, d)| n.len().max(d.len()).max(6))
            .collect();

        // Print header
        let header: Vec<String> = names
            .iter()
            .enumerate()
            .map(|(i, n)| format!("{:>width$}", n, width = widths[i]))
            .collect();
        writeln!(f, "{}", header.join(" | "))?;

        // Print dtypes
        let dtype_row: Vec<String> = dtypes
            .iter()
            .enumerate()
            .map(|(i, d)| format!("{:>width$}", d, width = widths[i]))
            .collect();
        writeln!(f, "{}", dtype_row.join(" | "))?;

        // Separator
        let sep: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();
        writeln!(f, "{}", sep.join("-+-"))?;

        // Data rows (up to 10)
        let n = self.height().min(10);
        for row in 0..n {
            let mut cells = Vec::new();
            for (ci, col) in self.columns.iter().enumerate() {
                let arr = col.to_array();
                let cell = if arr.is_null(row) {
                    "null".to_string()
                } else {
                    match col.dtype() {
                        DataType::Int64 => {
                            let p = arr
                                .as_any()
                                .downcast_ref::<arrow2::array::PrimitiveArray<i64>>()
                                .unwrap();
                            format!("{}", p.value(row))
                        }
                        DataType::Int32 => {
                            let p = arr
                                .as_any()
                                .downcast_ref::<arrow2::array::PrimitiveArray<i32>>()
                                .unwrap();
                            format!("{}", p.value(row))
                        }
                        DataType::Float64 => {
                            let p = arr
                                .as_any()
                                .downcast_ref::<arrow2::array::PrimitiveArray<f64>>()
                                .unwrap();
                            format!("{:.1}", p.value(row))
                        }
                        DataType::Float32 => {
                            let p = arr
                                .as_any()
                                .downcast_ref::<arrow2::array::PrimitiveArray<f32>>()
                                .unwrap();
                            format!("{:.1}", p.value(row))
                        }
                        DataType::Boolean => {
                            let p = arr
                                .as_any()
                                .downcast_ref::<arrow2::array::BooleanArray>()
                                .unwrap();
                            format!("{}", p.value(row))
                        }
                        DataType::Utf8 => {
                            let p = arr
                                .as_any()
                                .downcast_ref::<arrow2::array::Utf8Array<i32>>()
                                .unwrap();
                            format!("{:?}", p.value(row))
                        }
                        _ => "...".to_string(),
                    }
                };
                cells.push(format!("{:>width$}", cell, width = widths[ci]));
            }
            writeln!(f, "{}", cells.join(" | "))?;
        }
        if self.height() > 10 {
            writeln!(f, "... ({} more rows)", self.height() - 10)?;
        }
        Ok(())
    }
}

impl fmt::Debug for DataFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}
