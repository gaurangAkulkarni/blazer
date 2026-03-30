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

            // Use a single-row-group FileReader to read this row group
            let reader = self.current_reader.as_mut().unwrap();
            let rg_reader = pq_read::FileReader::new(
                reader,
                vec![rg.clone()],
                arrow_schema.clone(),
                self.chunk_rows,
                None, // limit
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

            // Build DataFrame
            let mut series_vec = Vec::with_capacity(arrow_schema.fields.len());
            for (i, field) in arrow_schema.fields.iter().enumerate() {
                let s = if columns[i].len() == 1 {
                    Series::from_arrow(&field.name, columns[i][0].clone())?
                } else {
                    Series::from_chunks(&field.name, columns[i].clone())?
                };
                series_vec.push(s);
            }

            let df = DataFrame::new(series_vec)?;

            // Apply projection if specified
            let df = if let Some(ref proj) = self.projection {
                let proj_refs: Vec<&str> = proj.iter().map(|s| s.as_str()).collect();
                df.select_columns(&proj_refs)?
            } else {
                df
            };

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
