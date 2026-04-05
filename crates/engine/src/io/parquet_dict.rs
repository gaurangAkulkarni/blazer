//! Dictionary-page DISTINCT optimisation
//!
//! For `SELECT [cols] DISTINCT` on a parquet dataset where every projected
//! column is dictionary-encoded, we can skip all data pages and enumerate
//! unique values by reading only the tiny dictionary page of each row group.
//!
//! For VendorID (2 unique values across 46 M rows / 92 row groups):
//!   Normal scan : 92 × 500K rows × 4 B = ~184 MB decompressed
//!   Dict scan   : 92 × 2 entries × 4 B = ~736 B read
//!
//! # Fallback
//! Returns `Ok(None)` whenever any projected column in any file lacks a
//! dictionary page (plain-encoded, delta-encoded, …).  The caller then falls
//! back to the normal streaming scan automatically.

use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::Arc;

use arrow2::io::parquet::read as pq_read;
use parquet2::read::{decompress as pq_decompress, get_page_iterator};
use parquet2::page::{CompressedPage, Page};
use parquet2::schema::types::PhysicalType;

use crate::dataframe::DataFrame;
use crate::error::{BlazeError, Result};
use crate::series::Series;

// ── Per-column accumulator ────────────────────────────────────────────────────

/// Tracks unique values for one column using raw bytes as the canonical key.
/// Maintains insertion order so the output is deterministic.
struct ColAcc {
    physical_type: PhysicalType,
    seen: ahash::AHashSet<Vec<u8>>,
    entries: Vec<Vec<u8>>,
}

impl ColAcc {
    fn new(pt: PhysicalType) -> Self {
        ColAcc {
            physical_type: pt,
            seen: ahash::AHashSet::default(),
            entries: Vec::new(),
        }
    }

    fn insert(&mut self, bytes: Vec<u8>) {
        if self.seen.insert(bytes.clone()) {
            self.entries.push(bytes);
        }
    }
}

// ── Public entry point ────────────────────────────────────────────────────────

/// Try to satisfy `SELECT columns DISTINCT` using only parquet dictionary pages.
///
/// * Returns `Ok(Some(df))` when every projected column in every file is
///   dictionary-encoded.  `df` contains exactly the unique values across all
///   files and row groups with no duplicates.
/// * Returns `Ok(None)` when any column is not dictionary-encoded; the caller
///   must fall back to a full streaming scan.
/// * Returns `Err(…)` on genuine I/O or decoding failures.
pub fn try_dict_distinct(
    files: &[PathBuf],
    columns: &[String],
) -> Result<Option<DataFrame>> {
    if files.is_empty() || columns.is_empty() {
        return Ok(None);
    }

    // One accumulator per projected column, initialised lazily on the first
    // row group we encounter.
    let mut accs: Vec<Option<ColAcc>> = (0..columns.len()).map(|_| None).collect();

    for file_path in files {
        let file = File::open(file_path).map_err(BlazeError::Io)?;
        let mut reader = BufReader::new(file);
        let metadata = pq_read::read_metadata(&mut reader)?;
        let arrow_schema = pq_read::infer_schema(&metadata)?;

        // Map each requested column name to its index in the parquet schema.
        let col_indices: Option<Vec<usize>> = columns
            .iter()
            .map(|name| {
                arrow_schema
                    .fields
                    .iter()
                    .position(|f| f.name.eq_ignore_ascii_case(name.as_str()))
            })
            .collect();

        let col_indices = match col_indices {
            Some(v) => v,
            None => return Ok(None), // a column is missing from this file
        };

        for rg in &metadata.row_groups {
            for (acc_idx, &schema_idx) in col_indices.iter().enumerate() {
                let col_meta = &rg.columns()[schema_idx];

                // Fast pre-check: if there is no dictionary page offset recorded
                // in the column chunk metadata the column is not dict-encoded.
                if col_meta.dictionary_page_offset().is_none() {
                    return Ok(None);
                }

                let physical_type = col_meta.physical_type();

                // Initialise the accumulator from the first row group we see.
                if accs[acc_idx].is_none() {
                    match physical_type {
                        PhysicalType::Int32
                        | PhysicalType::Int64
                        | PhysicalType::Float
                        | PhysicalType::Double
                        | PhysicalType::ByteArray => {
                            accs[acc_idx] = Some(ColAcc::new(physical_type));
                        }
                        // We only support the common scalar types for now.
                        _ => return Ok(None),
                    }
                }

                // Read and decode the dictionary page for this column chunk.
                match read_dict_entries(col_meta, &mut reader, physical_type)? {
                    Some(entries) => {
                        let acc = accs[acc_idx].as_mut().unwrap();
                        for entry in entries {
                            acc.insert(entry);
                        }
                    }
                    None => return Ok(None), // not dict-encoded after all
                }
            }
        }
    }

    // Convert each accumulator into a Series.
    let mut series_vec = Vec::with_capacity(columns.len());
    for (i, acc_opt) in accs.into_iter().enumerate() {
        match acc_opt {
            Some(acc) => series_vec.push(acc_to_series(&columns[i], acc)?),
            None => return Ok(None), // no row groups were found
        }
    }

    Ok(Some(DataFrame::new(series_vec)?))
}

// ── Read the dictionary page for one column chunk ─────────────────────────────

/// Open a page iterator for `col_meta`, read the first (dictionary) page,
/// decompress it, and return the PLAIN-encoded entries as raw byte vectors.
///
/// Returns `None` if the first page is a data page (not dict-encoded).
fn read_dict_entries<R: std::io::Read + std::io::Seek>(
    col_meta: &parquet2::metadata::ColumnChunkMetaData,
    reader: &mut R,
    physical_type: PhysicalType,
) -> Result<Option<Vec<Vec<u8>>>> {
    // get_page_iterator takes ownership of the reader, so we need a wrapper.
    // We use a mutable reference adapter via a helper struct.
    let mut page_iter =
        get_page_iterator(col_meta, &mut *reader, None, Vec::new(), 1 << 20)
            .map_err(|e| BlazeError::ComputeError(format!("parquet page iter: {e}")))?;

    // The very first page of a dict-encoded column chunk is the dictionary page.
    match page_iter.next() {
        None => Ok(None),
        Some(Err(e)) => Err(BlazeError::ComputeError(format!("parquet page read: {e}"))),
        Some(Ok(CompressedPage::Data(_))) => {
            // First page is a data page — column is NOT dictionary-encoded.
            Ok(None)
        }
        Some(Ok(compressed_dict)) => {
            // Decompress into a Page::Dict.
            let mut scratch = Vec::new();
            let page = pq_decompress(compressed_dict, &mut scratch)
                .map_err(|e| BlazeError::ComputeError(format!("parquet decompress: {e}")))?;
            match page {
                Page::Dict(dp) => {
                    let entries = decode_plain_dict(&dp.buffer, dp.num_values, physical_type)?;
                    Ok(Some(entries))
                }
                Page::Data(_) => Ok(None),
            }
        }
    }
}

// ── PLAIN dictionary buffer decoder ──────────────────────────────────────────

/// Decode a PLAIN-encoded dictionary page buffer into canonical byte slices,
/// one entry per unique dictionary value.
///
/// PLAIN encoding for each physical type:
///   INT32   — 4-byte little-endian i32 values
///   INT64   — 8-byte little-endian i64 values
///   FLOAT   — 4-byte IEEE-754 f32 values
///   DOUBLE  — 8-byte IEEE-754 f64 values
///   BYTE_ARRAY — `[u32_len][bytes]` pairs
fn decode_plain_dict(
    buffer: &[u8],
    num_values: usize,
    physical_type: PhysicalType,
) -> Result<Vec<Vec<u8>>> {
    match physical_type {
        PhysicalType::Int32 => {
            let stride = 4;
            if buffer.len() < num_values * stride {
                return Err(BlazeError::ComputeError(format!(
                    "INT32 dict buffer too small: {} < {}",
                    buffer.len(),
                    num_values * stride
                )));
            }
            Ok(buffer
                .chunks_exact(stride)
                .take(num_values)
                .map(|b| b.to_vec())
                .collect())
        }
        PhysicalType::Int64 => {
            let stride = 8;
            if buffer.len() < num_values * stride {
                return Err(BlazeError::ComputeError(
                    "INT64 dict buffer too small".into(),
                ));
            }
            Ok(buffer
                .chunks_exact(stride)
                .take(num_values)
                .map(|b| b.to_vec())
                .collect())
        }
        PhysicalType::Float => {
            let stride = 4;
            if buffer.len() < num_values * stride {
                return Err(BlazeError::ComputeError(
                    "FLOAT dict buffer too small".into(),
                ));
            }
            Ok(buffer
                .chunks_exact(stride)
                .take(num_values)
                .map(|b| b.to_vec())
                .collect())
        }
        PhysicalType::Double => {
            let stride = 8;
            if buffer.len() < num_values * stride {
                return Err(BlazeError::ComputeError(
                    "DOUBLE dict buffer too small".into(),
                ));
            }
            Ok(buffer
                .chunks_exact(stride)
                .take(num_values)
                .map(|b| b.to_vec())
                .collect())
        }
        PhysicalType::ByteArray => {
            let mut entries = Vec::with_capacity(num_values);
            let mut offset = 0usize;
            while entries.len() < num_values {
                if offset + 4 > buffer.len() {
                    break;
                }
                let len = u32::from_le_bytes(
                    buffer[offset..offset + 4].try_into().unwrap(),
                ) as usize;
                offset += 4;
                if offset + len > buffer.len() {
                    break;
                }
                entries.push(buffer[offset..offset + len].to_vec());
                offset += len;
            }
            Ok(entries)
        }
        _ => Err(BlazeError::ComputeError(format!(
            "unsupported physical type for dict decode: {:?}",
            physical_type
        ))),
    }
}

// ── Convert accumulator to Series ─────────────────────────────────────────────

fn acc_to_series(name: &str, acc: ColAcc) -> Result<Series> {
    match acc.physical_type {
        PhysicalType::Int32 => {
            let values: Vec<i32> = acc
                .entries
                .iter()
                .map(|b| i32::from_le_bytes(b[..4].try_into().unwrap()))
                .collect();
            let array = Arc::new(arrow2::array::Int32Array::from_vec(values));
            Series::from_arrow(name, array)
        }
        PhysicalType::Int64 => {
            let values: Vec<i64> = acc
                .entries
                .iter()
                .map(|b| i64::from_le_bytes(b[..8].try_into().unwrap()))
                .collect();
            let array = Arc::new(arrow2::array::Int64Array::from_vec(values));
            Series::from_arrow(name, array)
        }
        PhysicalType::Float => {
            let values: Vec<f32> = acc
                .entries
                .iter()
                .map(|b| f32::from_le_bytes(b[..4].try_into().unwrap()))
                .collect();
            let array = Arc::new(arrow2::array::Float32Array::from_vec(values));
            Series::from_arrow(name, array)
        }
        PhysicalType::Double => {
            let values: Vec<f64> = acc
                .entries
                .iter()
                .map(|b| f64::from_le_bytes(b[..8].try_into().unwrap()))
                .collect();
            let array = Arc::new(arrow2::array::Float64Array::from_vec(values));
            Series::from_arrow(name, array)
        }
        PhysicalType::ByteArray => {
            // Assume BYTE_ARRAY is valid UTF-8 (Utf8 logical type in parquet).
            let strings: Vec<Option<&str>> = acc
                .entries
                .iter()
                .map(|b| std::str::from_utf8(b).ok())
                .collect();
            if strings.iter().any(|s| s.is_none()) {
                return Err(BlazeError::ComputeError(
                    "dict column contains non-UTF-8 bytes; cannot build Utf8 Series".into(),
                ));
            }
            let array = Arc::new(arrow2::array::Utf8Array::<i32>::from(strings));
            Series::from_arrow(name, array)
        }
        _ => Err(BlazeError::ComputeError(format!(
            "unsupported physical type in acc_to_series: {:?}",
            acc.physical_type
        ))),
    }
}
