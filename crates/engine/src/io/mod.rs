pub mod parquet_dict;
pub mod parquet_stream;
pub mod sink;
pub mod spill;

use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, Write};
use std::path::Path;
use std::sync::Arc;

use arrow2::io::csv::read as csv_read;
use arrow2::io::ipc::read as ipc_read;
use arrow2::io::ipc::write as ipc_write;
use arrow2::io::parquet::read as pq_read;
use arrow2::io::parquet::read::statistics as pq_stats;
use arrow2::io::parquet::write as pq_write;

use crate::dataframe::DataFrame;
use crate::dtype::DataType;
use crate::error::Result;
use crate::schema::Schema;
use crate::series::Series;

// ---- CSV Reader ----

pub struct CsvReader {
    reader: BufReader<File>,
    has_header: bool,
    delimiter: u8,
}

impl CsvReader {
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path.as_ref())?;
        Ok(CsvReader {
            reader: BufReader::new(file),
            has_header: true,
            delimiter: b',',
        })
    }

    pub fn has_header(mut self, has: bool) -> Self {
        self.has_header = has;
        self
    }

    pub fn delimiter(mut self, delim: u8) -> Self {
        self.delimiter = delim;
        self
    }

    pub fn finish(mut self) -> Result<DataFrame> {
        let mut csv_reader = csv_read::ReaderBuilder::new()
            .has_headers(self.has_header)
            .delimiter(self.delimiter)
            .from_reader(&mut self.reader);

        // Infer schema
        let (fields, _) = csv_read::infer_schema(&mut csv_reader, None, self.has_header, &csv_read::infer)?;

        // Read all records
        let mut rows = Vec::new();
        loop {
            let mut record = csv_read::ByteRecord::new();
            match csv_reader.read_byte_record(&mut record) {
                Ok(true) => rows.push(record),
                Ok(false) => break,
                Err(_) => break,
            }
        }

        if rows.is_empty() {
            return Ok(DataFrame::empty());
        }

        // Deserialize
        let arrays = csv_read::deserialize_batch(
            &rows,
            &fields,
            None,
            0,
            csv_read::deserialize_column,
        )?;

        // Convert Box<dyn Array> to Series
        let mut columns = Vec::with_capacity(fields.len());
        for (i, field) in fields.iter().enumerate() {
            let array: Arc<dyn arrow2::array::Array> = arrays[i].clone().into();
            let series = Series::from_arrow(&field.name, array)?;
            columns.push(series);
        }

        DataFrame::new(columns)
    }
}

// ---- CSV Writer ----

pub struct CsvWriter {
    writer: BufWriter<File>,
    delimiter: u8,
}

impl CsvWriter {
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::create(path.as_ref())?;
        Ok(CsvWriter {
            writer: BufWriter::new(file),
            delimiter: b',',
        })
    }

    pub fn delimiter(mut self, delim: u8) -> Self {
        self.delimiter = delim;
        self
    }

    pub fn finish(mut self, df: &DataFrame) -> Result<()> {
        let schema = df.schema().to_arrow();
        let delim = self.delimiter as char;

        // Write header
        let names: Vec<&str> = schema.fields.iter().map(|f| f.name.as_str()).collect();
        writeln!(self.writer, "{}", names.join(&String::from(delim)))?;

        // Write data rows
        let height = df.height();

        for row in 0..height {
            let mut cells: Vec<String> = Vec::with_capacity(df.width());
            for col in df.columns() {
                let arr = col.to_array();
                if arr.is_null(row) {
                    cells.push(String::new());
                } else {
                    let cell = match col.dtype() {
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
                            format!("{}", p.value(row))
                        }
                        DataType::Float32 => {
                            let p = arr
                                .as_any()
                                .downcast_ref::<arrow2::array::PrimitiveArray<f32>>()
                                .unwrap();
                            format!("{}", p.value(row))
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
                            p.value(row).to_string()
                        }
                        _ => String::new(),
                    };
                    cells.push(cell);
                }
            }
            writeln!(self.writer, "{}", cells.join(&String::from(delim)))?;
        }

        self.writer.flush()?;
        Ok(())
    }
}

// ---- Row-group predicate types (zone-map skipping) ----------------------

/// Comparison operator for a row-group filter.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RgOp {
    Gt,
    GtEq,
    Lt,
    LtEq,
    Eq,
}

/// Literal value for a row-group filter.
#[derive(Debug, Clone)]
pub enum RgValue {
    Int64(i64),
    Float64(f64),
    Utf8(String),
}

/// A simple `column OP value` predicate used for zone-map (min/max stats) skipping.
/// Only AND-combinations of these are used — OR is conservatively ignored.
#[derive(Debug, Clone)]
pub struct RgPredicate {
    pub column: String,
    pub op: RgOp,
    pub value: RgValue,
}

/// Returns the indices of row groups that **cannot be skipped** for the given predicates.
fn surviving_row_group_indices(
    row_groups: &[pq_read::RowGroupMetaData],
    arrow_schema: &arrow2::datatypes::Schema,
    predicates: &[RgPredicate],
) -> Vec<usize> {
    let n = row_groups.len();
    let mut keep = vec![true; n];

    for pred in predicates {
        let col_lower = pred.column.to_lowercase();
        let field = match arrow_schema
            .fields
            .iter()
            .find(|f| f.name.to_lowercase() == col_lower)
        {
            Some(f) => f.clone(),
            None => continue, // unknown column — keep all
        };

        let stats = match pq_stats::deserialize(&field, row_groups) {
            Ok(s) => s,
            Err(_) => continue, // no stats available — keep all
        };

        for rg_idx in 0..n {
            if !keep[rg_idx] {
                continue;
            }
            if rg_can_skip(pred, &*stats.min_value, &*stats.max_value, rg_idx) {
                keep[rg_idx] = false;
            }
        }
    }

    keep.iter()
        .enumerate()
        .filter(|(_, k)| **k)
        .map(|(i, _)| i)
        .collect()
}

/// Returns `true` if this row group definitely contains no rows matching `pred`.
fn rg_can_skip(
    pred: &RgPredicate,
    min_arr: &dyn arrow2::array::Array,
    max_arr: &dyn arrow2::array::Array,
    idx: usize,
) -> bool {
    // Missing statistics → cannot safely skip
    if idx >= min_arr.len() || idx >= max_arr.len() {
        return false;
    }
    if min_arr.is_null(idx) || max_arr.is_null(idx) {
        return false;
    }

    // Numeric path: convert min/max to f64 and compare
    if let (Some(min_f), Some(max_f)) =
        (arr_as_f64(min_arr, idx), arr_as_f64(max_arr, idx))
    {
        if let Some(v) = rg_value_as_f64(&pred.value) {
            return match pred.op {
                RgOp::Gt   => max_f <= v,              // no value > v
                RgOp::GtEq => max_f < v,               // no value >= v
                RgOp::Lt   => min_f >= v,               // no value < v
                RgOp::LtEq => min_f > v,               // no value <= v
                RgOp::Eq   => v < min_f || v > max_f,  // v outside [min, max]
            };
        }
    }

    // String path
    if let (Some(min_s), Some(max_s)) =
        (arr_as_str(min_arr, idx), arr_as_str(max_arr, idx))
    {
        if let RgValue::Utf8(ref v) = pred.value {
            return match pred.op {
                RgOp::Gt   => max_s.as_str() <= v.as_str(),
                RgOp::GtEq => max_s.as_str() < v.as_str(),
                RgOp::Lt   => min_s.as_str() >= v.as_str(),
                RgOp::LtEq => min_s.as_str() > v.as_str(),
                RgOp::Eq   => v.as_str() < min_s.as_str() || v.as_str() > max_s.as_str(),
            };
        }
    }

    false // unknown type → keep the row group
}

fn arr_as_f64(arr: &dyn arrow2::array::Array, idx: usize) -> Option<f64> {
    use arrow2::array::PrimitiveArray;
    use arrow2::datatypes::DataType as AD;
    match arr.data_type() {
        AD::Float64 => arr.as_any().downcast_ref::<PrimitiveArray<f64>>().map(|a| a.value(idx)),
        AD::Float32 => arr.as_any().downcast_ref::<PrimitiveArray<f32>>().map(|a| a.value(idx) as f64),
        AD::Int64 | AD::Timestamp(_, _) | AD::Date64 => {
            arr.as_any().downcast_ref::<PrimitiveArray<i64>>().map(|a| a.value(idx) as f64)
        }
        AD::Int32 | AD::Date32 => {
            arr.as_any().downcast_ref::<PrimitiveArray<i32>>().map(|a| a.value(idx) as f64)
        }
        AD::UInt64 => arr.as_any().downcast_ref::<PrimitiveArray<u64>>().map(|a| a.value(idx) as f64),
        AD::UInt32 => arr.as_any().downcast_ref::<PrimitiveArray<u32>>().map(|a| a.value(idx) as f64),
        _ => None,
    }
}

fn arr_as_str(arr: &dyn arrow2::array::Array, idx: usize) -> Option<String> {
    use arrow2::array::Utf8Array;
    use arrow2::datatypes::DataType as AD;
    match arr.data_type() {
        AD::Utf8 => arr.as_any().downcast_ref::<Utf8Array<i32>>().map(|a| a.value(idx).to_string()),
        AD::LargeUtf8 => arr.as_any().downcast_ref::<Utf8Array<i64>>().map(|a| a.value(idx).to_string()),
        _ => None,
    }
}

fn rg_value_as_f64(v: &RgValue) -> Option<f64> {
    match v {
        RgValue::Int64(i) => Some(*i as f64),
        RgValue::Float64(f) => Some(*f),
        RgValue::Utf8(_) => None,
    }
}

// ---- Parquet Reader ----

pub struct ParquetReader<R: Read + Seek> {
    reader: R,
    projection: Option<Vec<String>>,
    n_rows: Option<usize>,
    /// Zone-map predicates for row-group skipping.
    row_group_predicates: Vec<RgPredicate>,
}

impl ParquetReader<BufReader<File>> {
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path.as_ref())?;
        Ok(ParquetReader {
            reader: BufReader::new(file),
            projection: None,
            n_rows: None,
            row_group_predicates: Vec::new(),
        })
    }
}

impl<R: Read + Seek> ParquetReader<R> {
    pub fn new(reader: R) -> Self {
        ParquetReader {
            reader,
            projection: None,
            n_rows: None,
            row_group_predicates: Vec::new(),
        }
    }

    pub fn with_projection(mut self, cols: Vec<String>) -> Self {
        self.projection = Some(cols);
        self
    }

    pub fn with_n_rows(mut self, n: usize) -> Self {
        self.n_rows = Some(n);
        self
    }

    /// Attach zone-map predicates. Row groups whose min/max statistics prove
    /// that no row can satisfy ALL predicates will be skipped entirely.
    pub fn with_row_group_predicates(mut self, preds: Vec<RgPredicate>) -> Self {
        self.row_group_predicates = preds;
        self
    }

    pub fn finish(mut self) -> Result<DataFrame> {
        let metadata = pq_read::read_metadata(&mut self.reader)?;
        let arrow_schema = pq_read::infer_schema(&metadata)?;

        // Build the schema to actually read — either the full schema or a
        // column-filtered subset. arrow2's FileReader matches columns by name,
        // so passing a reduced schema causes it to read only those columns.
        let read_schema = if let Some(ref proj) = self.projection {
            // Match projected column names case-insensitively against the file schema
            let proj_lower: Vec<String> = proj.iter().map(|s| s.to_lowercase()).collect();
            let filtered_fields: Vec<_> = arrow_schema
                .fields
                .iter()
                .filter(|f| proj_lower.contains(&f.name.to_lowercase()))
                .cloned()
                .collect();
            arrow2::datatypes::Schema {
                fields: filtered_fields,
                metadata: arrow_schema.metadata.clone(),
            }
        } else {
            arrow_schema.clone()
        };

        let fields = read_schema.fields.clone();

        // ── Zone-map row-group skipping ──────────────────────────────────────
        // Use the full arrow_schema (not the projected one) for statistics
        // look-up so we can filter on columns that aren't projected.
        let row_groups = if !self.row_group_predicates.is_empty() {
            let surviving = surviving_row_group_indices(
                &metadata.row_groups,
                &arrow_schema,
                &self.row_group_predicates,
            );
            let surviving_set: std::collections::HashSet<usize> =
                surviving.into_iter().collect();
            metadata
                .row_groups
                .into_iter()
                .enumerate()
                .filter(|(i, _)| surviving_set.contains(i))
                .map(|(_, rg)| rg)
                .collect()
        } else {
            metadata.row_groups
        };

        let file_reader = pq_read::FileReader::new(
            &mut self.reader,
            row_groups,
            read_schema, // ← filtered schema so only projected cols are read
            None,        // chunk_size
            self.n_rows,
            None, // page_indexes
        );

        let mut all_columns: Vec<Vec<Arc<dyn arrow2::array::Array>>> = Vec::new();

        for chunk_result in file_reader {
            let chunk = chunk_result?;
            if all_columns.is_empty() {
                for _ in 0..chunk.arrays().len() {
                    all_columns.push(Vec::new());
                }
            }
            for (i, array) in chunk.into_arrays().into_iter().enumerate() {
                all_columns[i].push(array.into());
            }
        }

        if all_columns.is_empty() {
            return Ok(DataFrame::empty());
        }

        let mut series_vec = Vec::with_capacity(fields.len());
        for (i, field) in fields.iter().enumerate() {
            let s = if all_columns[i].len() == 1 {
                Series::from_arrow(&field.name, all_columns[i][0].clone())?
            } else {
                Series::from_chunks(&field.name, all_columns[i].clone())?
            };
            series_vec.push(s);
        }

        DataFrame::new(series_vec)
    }
}

/// Read Parquet file metadata without loading data.
pub fn read_parquet_metadata<R: Read + Seek>(
    reader: &mut R,
) -> Result<pq_read::FileMetaData> {
    Ok(pq_read::read_metadata(reader)?)
}

/// Infer a Schema from Parquet metadata.
pub fn infer_parquet_schema(metadata: &pq_read::FileMetaData) -> Result<Schema> {
    let arrow_schema = pq_read::infer_schema(metadata)?;
    Ok(Schema::from_arrow(&arrow_schema))
}

// ---- Parquet Writer ----

pub struct ParquetWriter {
    path: std::path::PathBuf,
    compression: pq_write::CompressionOptions,
}

impl ParquetWriter {
    pub fn from_path<P: AsRef<Path>>(path: P) -> Self {
        ParquetWriter {
            path: path.as_ref().to_path_buf(),
            compression: pq_write::CompressionOptions::Uncompressed,
        }
    }

    pub fn with_compression(mut self, compression: pq_write::CompressionOptions) -> Self {
        self.compression = compression;
        self
    }

    pub fn finish(&self, df: &DataFrame) -> Result<()> {
        const ROW_GROUP_SIZE: usize = 500_000;

        let arrow_schema = df.schema().to_arrow();
        let options = pq_write::WriteOptions {
            write_statistics: true,
            compression: self.compression,
            version: pq_write::Version::V1,
            data_pagesize_limit: None,
        };
        let encodings: Vec<Vec<pq_write::Encoding>> = arrow_schema
            .fields
            .iter()
            .map(|_| vec![pq_write::Encoding::Plain])
            .collect();

        let file = File::create(&self.path)?;
        let mut writer = pq_write::FileWriter::try_new(
            BufWriter::new(file),
            arrow_schema.clone(),
            options,
        )?;

        // Pre-extract all column arrays once
        let col_arrays: Vec<Arc<dyn arrow2::array::Array>> =
            df.columns().iter().map(|s| s.to_array()).collect();

        let height = df.height();
        let mut offset = 0;
        while offset < height {
            let length = ROW_GROUP_SIZE.min(height - offset);

            // Slice each column array for this row group
            let sliced: Vec<Box<dyn arrow2::array::Array>> = col_arrays
                .iter()
                .map(|arr| {
                    let s: Box<dyn arrow2::array::Array> = arr.as_ref().sliced(offset, length);
                    arrow2::array::clone(s.as_ref())
                })
                .collect();

            let chunk = arrow2::chunk::Chunk::new(sliced);
            let row_group_iter = pq_write::RowGroupIterator::try_new(
                std::iter::once(Ok(chunk)),
                &arrow_schema,
                options,
                encodings.clone(),
            )?;
            for group in row_group_iter {
                writer.write(group?)?;
            }
            offset += length;
        }

        writer.end(None)?;
        Ok(())
    }
}

/// Write a DataFrame to a Parquet file (convenience function).
pub fn write_parquet(df: &DataFrame, path: &str) -> Result<()> {
    ParquetWriter::from_path(path).finish(df)
}

/// Write a single chunk to an open Parquet FileWriter.
pub fn write_chunk_to_parquet<W: Write>(
    writer: &mut pq_write::FileWriter<BufWriter<W>>,
    df: &DataFrame,
    arrow_schema: &arrow2::datatypes::Schema,
    options: pq_write::WriteOptions,
) -> Result<()>
where
    W: Write,
{
    let encodings: Vec<Vec<pq_write::Encoding>> = arrow_schema
        .fields
        .iter()
        .map(|_| vec![pq_write::Encoding::Plain])
        .collect();

    let arrays: Vec<Box<dyn arrow2::array::Array>> = df
        .columns()
        .iter()
        .map(|s| arrow2::array::clone(s.to_array().as_ref()))
        .collect();

    let chunk = arrow2::chunk::Chunk::new(arrays);

    let row_group_iter = pq_write::RowGroupIterator::try_new(
        std::iter::once(Ok(chunk)),
        arrow_schema,
        options,
        encodings,
    )?;

    for group in row_group_iter {
        writer.write(group?)?;
    }
    Ok(())
}

// ---- IPC Reader ----

pub struct IpcReader<R: Read + Seek> {
    reader: R,
}

impl IpcReader<BufReader<File>> {
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path.as_ref())?;
        Ok(IpcReader {
            reader: BufReader::new(file),
        })
    }
}

impl<R: Read + Seek> IpcReader<R> {
    pub fn new(reader: R) -> Self {
        IpcReader { reader }
    }

    pub fn finish(mut self) -> Result<DataFrame> {
        let metadata = ipc_read::read_file_metadata(&mut self.reader)?;
        let arrow_schema = metadata.schema.clone();

        let file_reader = ipc_read::FileReader::new(
            &mut self.reader,
            metadata,
            None, // projection
            None, // limit
        );

        let mut all_columns: Vec<Vec<Arc<dyn arrow2::array::Array>>> = Vec::new();

        for chunk_result in file_reader {
            let chunk = chunk_result?;
            if all_columns.is_empty() {
                for _ in 0..chunk.arrays().len() {
                    all_columns.push(Vec::new());
                }
            }
            for (i, array) in chunk.into_arrays().into_iter().enumerate() {
                all_columns[i].push(array.into());
            }
        }

        if all_columns.is_empty() {
            return Ok(DataFrame::empty());
        }

        let mut series_vec = Vec::with_capacity(arrow_schema.fields.len());
        for (i, field) in arrow_schema.fields.iter().enumerate() {
            let s = if all_columns[i].len() == 1 {
                Series::from_arrow(&field.name, all_columns[i][0].clone())?
            } else {
                Series::from_chunks(&field.name, all_columns[i].clone())?
            };
            series_vec.push(s);
        }

        DataFrame::new(series_vec)
    }
}

// ---- IPC Writer ----

pub struct IpcWriter {
    path: std::path::PathBuf,
}

impl IpcWriter {
    pub fn from_path<P: AsRef<Path>>(path: P) -> Self {
        IpcWriter {
            path: path.as_ref().to_path_buf(),
        }
    }

    pub fn finish(&self, df: &DataFrame) -> Result<()> {
        let arrow_schema = df.schema().to_arrow();
        let options = ipc_write::WriteOptions {
            compression: None,
        };

        let file = File::create(&self.path)?;
        let mut writer = ipc_write::FileWriter::try_new(
            BufWriter::new(file),
            arrow_schema.clone(),
            None, // ipc_fields
            options,
        )?;

        let arrays: Vec<Box<dyn arrow2::array::Array>> = df
            .columns()
            .iter()
            .map(|s| arrow2::array::clone(s.to_array().as_ref()))
            .collect();

        let chunk = arrow2::chunk::Chunk::new(arrays);
        writer.write(&chunk, None)?;
        writer.finish()?;
        Ok(())
    }
}

/// Write a DataFrame to an IPC file (convenience function).
pub fn write_ipc(df: &DataFrame, path: &str) -> Result<()> {
    IpcWriter::from_path(path).finish(df)
}

/// Read a DataFrame from an IPC file (convenience function).
pub fn read_ipc(path: &str) -> Result<DataFrame> {
    IpcReader::from_path(path)?.finish()
}
