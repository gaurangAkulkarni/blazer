use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::Arc;

use arrow2::io::parquet::read as pq_read;

use crate::dataframe::DataFrame;
use crate::error::Result;
use crate::expr::Expr;
use crate::schema::Schema;
use crate::series::Series;

/// Yields one Arrow row-group at a time.
/// Never loads more than one row group into memory simultaneously.
pub struct ParquetStream {
    files: Vec<PathBuf>,
    file_idx: usize,
    rg_idx: usize,
    projection: Option<Vec<String>>,
    predicate: Option<Expr>,
    schema: Option<Schema>,
    chunk_rows: Option<usize>,
    /// If set, the stream will stop reading pages inside a row group once
    /// this many rows have been produced in total across all row groups and
    /// files.  This mirrors the `limit` parameter of `pq_read::FileReader`
    /// and allows the parquet reader to skip page decompression once
    /// satisfied — exactly what DuckDB does for `SELECT … LIMIT n`.
    row_limit: Option<usize>,
    rows_emitted: usize,
    // Current file state
    current_reader: Option<BufReader<File>>,
    current_metadata: Option<pq_read::FileMetaData>,
    current_arrow_schema: Option<arrow2::datatypes::Schema>,
}

impl ParquetStream {
    pub fn new(files: Vec<PathBuf>) -> Self {
        ParquetStream {
            files,
            file_idx: 0,
            rg_idx: 0,
            projection: None,
            predicate: None,
            schema: None,
            chunk_rows: None,
            row_limit: None,
            rows_emitted: 0,
            current_reader: None,
            current_metadata: None,
            current_arrow_schema: None,
        }
    }

    pub fn with_projection(mut self, cols: Vec<String>) -> Self {
        self.projection = Some(cols);
        self
    }

    pub fn with_predicate(mut self, pred: Expr) -> Self {
        self.predicate = Some(pred);
        self
    }

    pub fn with_chunk_rows(mut self, n: usize) -> Self {
        self.chunk_rows = Some(n);
        self
    }

    /// Push the row-count limit down into the parquet page reader so that
    /// decompression stops as soon as `n` rows have been read, rather than
    /// always reading the full row group.
    pub fn with_row_limit(mut self, n: usize) -> Self {
        self.row_limit = Some(n);
        self
    }

    /// Open the file at file_idx and load its metadata.
    fn open_current_file(&mut self) -> Result<bool> {
        if self.file_idx >= self.files.len() {
            return Ok(false);
        }

        let file = File::open(&self.files[self.file_idx])?;
        let mut reader = BufReader::new(file);
        let metadata = pq_read::read_metadata(&mut reader)?;
        let arrow_schema = pq_read::infer_schema(&metadata)?;

        if self.schema.is_none() {
            self.schema = Some(Schema::from_arrow(&arrow_schema));
        }

        self.current_arrow_schema = Some(arrow_schema);
        self.current_metadata = Some(metadata);
        self.current_reader = Some(reader);
        self.rg_idx = 0;
        Ok(true)
    }

    /// Read a single row group from the current file.
    fn read_row_group(&mut self) -> Result<Option<DataFrame>> {
        loop {
            // If a global row limit was set and we've already hit it, stop.
            if let Some(lim) = self.row_limit {
                if self.rows_emitted >= lim {
                    return Ok(None);
                }
            }

            // Open file if needed
            if self.current_metadata.is_none() {
                if !self.open_current_file()? {
                    return Ok(None);
                }
            }

            let metadata = self.current_metadata.as_ref().unwrap();
            let arrow_schema = self.current_arrow_schema.as_ref().unwrap();

            // Check if we've exhausted row groups in current file
            if self.rg_idx >= metadata.row_groups.len() {
                self.current_reader = None;
                self.current_metadata = None;
                self.current_arrow_schema = None;
                self.file_idx += 1;
                continue; // Try next file
            }

            let rg = &metadata.row_groups[self.rg_idx];
            self.rg_idx += 1;

            // ── Column-level projection: build a schema that only includes the
            // requested columns.  The Parquet FileReader skips the byte ranges
            // of any column not present in the schema, so only the needed
            // column chunks are read and decompressed from disk.
            let read_schema: arrow2::datatypes::Schema = match &self.projection {
                None => arrow_schema.clone(),
                Some(proj) => {
                    let projected_fields: Vec<_> = arrow_schema
                        .fields
                        .iter()
                        .filter(|f| proj.iter().any(|p| p == &f.name))
                        .cloned()
                        .collect();
                    arrow2::datatypes::Schema {
                        fields: projected_fields,
                        metadata: arrow_schema.metadata.clone(),
                    }
                }
            };

            // Remaining rows we're allowed to read for this row group.
            // Passing this into FileReader stops page decompression as soon as
            // the parquet layer has decoded enough rows — this is the key
            // optimisation that gives DuckDB its sub-100ms LIMIT performance.
            let rg_limit: Option<usize> = self.row_limit.map(|lim| {
                lim.saturating_sub(self.rows_emitted)
            });

            // Use a single-row-group FileReader restricted to projected columns.
            let reader = self.current_reader.as_mut().unwrap();
            let rg_reader = pq_read::FileReader::new(
                reader,
                vec![rg.clone()],
                read_schema.clone(),
                self.chunk_rows,
                rg_limit, // ← was always None; now caps decompression at n rows
                None, // page_indexes
            );

            let mut columns: Vec<Vec<Arc<dyn arrow2::array::Array>>> = Vec::new();

            for chunk_result in rg_reader {
                let chunk = chunk_result?;
                if columns.is_empty() {
                    for _ in 0..chunk.arrays().len() {
                        columns.push(Vec::new());
                    }
                }
                for (i, array) in chunk.into_arrays().into_iter().enumerate() {
                    columns[i].push(array.into());
                }
            }

            if columns.is_empty() {
                continue;
            }

            // Build DataFrame directly from projected columns — no post-filter needed.
            let mut series_vec = Vec::with_capacity(read_schema.fields.len());
            for (i, field) in read_schema.fields.iter().enumerate() {
                let s = if columns[i].len() == 1 {
                    Series::from_arrow(&field.name, columns[i][0].clone())?
                } else {
                    Series::from_chunks(&field.name, columns[i].clone())?
                };
                series_vec.push(s);
            }

            let df = DataFrame::new(series_vec)?;
            self.rows_emitted += df.height();
            return Ok(Some(df));
        }
    }
}

impl Iterator for ParquetStream {
    type Item = Result<DataFrame>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.read_row_group() {
            Ok(Some(df)) => Some(Ok(df)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}
