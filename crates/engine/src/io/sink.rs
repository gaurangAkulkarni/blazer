use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use arrow2::io::parquet::write as pq_write;

use crate::dataframe::DataFrame;
use crate::error::Result;

/// Receives chunks and writes them to a destination without buffering all in RAM.
pub trait Sink: Send {
    fn write_chunk(&mut self, chunk: &DataFrame) -> Result<()>;
    fn finish(self: Box<Self>) -> Result<()>;
    fn name(&self) -> &'static str;
}

// ---- Parquet Sink ----

pub struct ParquetSink {
    path: PathBuf,
    tmp_path: PathBuf,
    writer: Option<pq_write::FileWriter<BufWriter<File>>>,
    arrow_schema: Option<arrow2::datatypes::Schema>,
    options: pq_write::WriteOptions,
    rows_written: usize,
}

impl ParquetSink {
    pub fn new(path: &str) -> Result<Self> {
        let path = PathBuf::from(path);
        let tmp_path = path.with_extension("parquet.tmp");
        Ok(ParquetSink {
            path,
            tmp_path,
            writer: None,
            arrow_schema: None,
            options: pq_write::WriteOptions {
                write_statistics: true,
                compression: pq_write::CompressionOptions::Uncompressed,
                version: pq_write::Version::V1,
                data_pagesize_limit: None,
            },
            rows_written: 0,
        })
    }

    pub fn rows_written(&self) -> usize {
        self.rows_written
    }

    fn ensure_writer(&mut self, df: &DataFrame) -> Result<()> {
        if self.writer.is_none() {
            let schema = df.schema().to_arrow();
            let file = File::create(&self.tmp_path)?;
            let writer = pq_write::FileWriter::try_new(
                BufWriter::new(file),
                schema.clone(),
                self.options,
            )?;
            self.arrow_schema = Some(schema);
            self.writer = Some(writer);
        }
        Ok(())
    }
}

impl Sink for ParquetSink {
    fn write_chunk(&mut self, chunk: &DataFrame) -> Result<()> {
        if chunk.height() == 0 {
            return Ok(());
        }
        self.ensure_writer(chunk)?;

        let schema = self.arrow_schema.as_ref().unwrap();
        crate::io::write_chunk_to_parquet(
            self.writer.as_mut().unwrap(),
            chunk,
            schema,
            self.options,
        )?;
        self.rows_written += chunk.height();
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<()> {
        if let Some(mut writer) = self.writer.take() {
            writer.end(None)?;
        }
        // Atomic rename: tmp -> final
        if self.tmp_path.exists() {
            std::fs::rename(&self.tmp_path, &self.path)?;
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        "ParquetSink"
    }
}

impl Drop for ParquetSink {
    fn drop(&mut self) {
        // Clean up tmp file if it still exists (error path)
        let _ = std::fs::remove_file(&self.tmp_path);
    }
}

// ---- CSV Sink ----

pub struct CsvSink {
    #[allow(dead_code)]
    path: PathBuf,
    file: BufWriter<File>,
    header_written: bool,
}

impl CsvSink {
    pub fn new(path: &str) -> Result<Self> {
        let file = File::create(path)?;
        Ok(CsvSink {
            path: PathBuf::from(path),
            file: BufWriter::new(file),
            header_written: false,
        })
    }
}

impl Sink for CsvSink {
    fn write_chunk(&mut self, chunk: &DataFrame) -> Result<()> {
        if !self.header_written {
            let names: Vec<&str> = chunk.get_column_names();
            writeln!(self.file, "{}", names.join(","))?;
            self.header_written = true;
        }

        for row in 0..chunk.height() {
            let mut cells: Vec<String> = Vec::with_capacity(chunk.width());
            for col in chunk.columns() {
                let arr = col.to_array();
                if arr.is_null(row) {
                    cells.push(String::new());
                } else {
                    let cell = match col.dtype() {
                        crate::dtype::DataType::Int64 => {
                            let p = arr.as_any().downcast_ref::<arrow2::array::PrimitiveArray<i64>>().unwrap();
                            format!("{}", p.value(row))
                        }
                        crate::dtype::DataType::Float64 => {
                            let p = arr.as_any().downcast_ref::<arrow2::array::PrimitiveArray<f64>>().unwrap();
                            format!("{}", p.value(row))
                        }
                        crate::dtype::DataType::Utf8 => {
                            let p = arr.as_any().downcast_ref::<arrow2::array::Utf8Array<i32>>().unwrap();
                            p.value(row).to_string()
                        }
                        _ => String::new(),
                    };
                    cells.push(cell);
                }
            }
            writeln!(self.file, "{}", cells.join(","))?;
        }
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<()> {
        self.file.flush()?;
        Ok(())
    }

    fn name(&self) -> &'static str {
        "CsvSink"
    }
}

// ---- Memory Sink ----

pub struct MemorySink {
    chunks: Vec<DataFrame>,
}

impl MemorySink {
    pub fn new() -> Self {
        MemorySink {
            chunks: Vec::new(),
        }
    }

    pub fn into_dataframe(self) -> Result<DataFrame> {
        if self.chunks.is_empty() {
            return Ok(DataFrame::empty());
        }
        let mut result = self.chunks[0].clone();
        for chunk in &self.chunks[1..] {
            result = result.vstack(chunk)?;
        }
        Ok(result)
    }
}

impl Sink for MemorySink {
    fn write_chunk(&mut self, chunk: &DataFrame) -> Result<()> {
        if chunk.height() > 0 {
            self.chunks.push(chunk.clone());
        }
        Ok(())
    }

    fn finish(self: Box<Self>) -> Result<()> {
        Ok(())
    }

    fn name(&self) -> &'static str {
        "MemorySink"
    }
}
