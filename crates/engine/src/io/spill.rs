use std::path::PathBuf;

use crate::dataframe::DataFrame;
use crate::error::Result;
use crate::io::{IpcReader, IpcWriter};

/// Manages temporary files for operations that exceed RAM budget.
/// Uses Arrow IPC format for fast serialization.
pub struct SpillManager {
    temp_dir: PathBuf,
    files: Vec<PathBuf>,
    budget: usize,
    used: usize,
    counter: usize,
}

impl SpillManager {
    /// Create a new SpillManager with the given RAM budget in bytes.
    pub fn new(budget: usize, temp_dir: Option<PathBuf>) -> Result<Self> {
        let dir = match temp_dir {
            Some(d) => {
                std::fs::create_dir_all(&d)?;
                d
            }
            None => std::env::temp_dir(),
        };
        Ok(SpillManager {
            temp_dir: dir,
            files: Vec::new(),
            budget,
            used: 0,
            counter: 0,
        })
    }

    /// Write a DataFrame to a temp Arrow IPC file. Returns the file path.
    pub fn spill(&mut self, df: &DataFrame) -> Result<PathBuf> {
        let pid = std::process::id();
        let path = self
            .temp_dir
            .join(format!("blaze_spill_{}_{}.arrow", pid, self.counter));
        self.counter += 1;

        IpcWriter::from_path(&path).finish(df)?;
        self.files.push(path.clone());
        Ok(path)
    }

    /// Read back a previously spilled DataFrame.
    pub fn read(&self, path: &PathBuf) -> Result<DataFrame> {
        IpcReader::from_path(path)?.finish()
    }

    /// Iterate over all spilled DataFrames in order.
    pub fn iter_spilled(&self) -> impl Iterator<Item = Result<DataFrame>> + '_ {
        self.files.iter().map(|p| self.read(p))
    }

    /// Delete all temp files.
    pub fn cleanup(&mut self) -> Result<()> {
        for path in self.files.drain(..) {
            let _ = std::fs::remove_file(path);
        }
        self.used = 0;
        Ok(())
    }

    /// Returns true if adding `bytes` more would exceed the budget.
    pub fn would_spill(&self, bytes: usize) -> bool {
        self.used + bytes > self.budget
    }

    /// Track bytes used in RAM.
    pub fn track_bytes(&mut self, bytes: usize) {
        self.used += bytes;
    }

    /// Release tracked bytes.
    pub fn release_bytes(&mut self, bytes: usize) {
        self.used = self.used.saturating_sub(bytes);
    }

    pub fn budget(&self) -> usize {
        self.budget
    }

    pub fn used(&self) -> usize {
        self.used
    }
}

impl Drop for SpillManager {
    fn drop(&mut self) {
        let _ = self.cleanup();
    }
}
