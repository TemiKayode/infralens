//! Write-Ahead Log (WAL) implementation.
//!
//! Entry format (little-endian):
//!   [CRC32: 4 bytes] [LEN: 4 bytes] [TYPE: 1 byte] [DATA: LEN bytes]
//!
//! CRC32 is computed over the bytes [TYPE || DATA].
//! A `Checkpoint` entry (type 0xFF) marks that all prior entries have been
//! durably written to an SSTable and can be ignored on recovery.

use crate::error::{Result, StorageError};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use tracing::{debug, warn};

// ── Entry type bytes ──────────────────────────────────────────────────────────

pub const ENTRY_LOG:        u8 = 0x01;
pub const ENTRY_METRIC:     u8 = 0x02;
pub const ENTRY_SPAN:       u8 = 0x03;
pub const ENTRY_CHECKPOINT: u8 = 0xFF;

const HEADER_SIZE: usize = 9; // crc32(4) + len(4) + type(1)

// ── WAL writer ───────────────────────────────────────────────────────────────

pub struct Wal {
    path:   PathBuf,
    writer: BufWriter<File>,
    /// Running count of bytes written (for offset reporting in errors).
    offset: u64,
}

impl Wal {
    /// Open or create a WAL file, seeking to end for appends.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_owned();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        let offset = file.metadata()?.len();
        Ok(Self {
            path,
            writer: BufWriter::with_capacity(256 * 1024, file),
            offset,
        })
    }

    /// Append a raw entry to the WAL.
    pub fn append(&mut self, entry_type: u8, data: &[u8]) -> Result<u64> {
        let start_offset = self.offset;
        let crc = crc32fast::hash(&[&[entry_type], data].concat());
        let len = data.len() as u32;

        let mut header = [0u8; HEADER_SIZE];
        header[0..4].copy_from_slice(&crc.to_le_bytes());
        header[4..8].copy_from_slice(&len.to_le_bytes());
        header[8] = entry_type;

        self.writer.write_all(&header)?;
        self.writer.write_all(data)?;
        self.offset += (HEADER_SIZE + data.len()) as u64;
        debug!(offset = start_offset, len, "WAL entry appended");
        Ok(start_offset)
    }

    /// Flush the BufWriter and fsync to durable storage.
    pub fn sync(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_data()?;
        Ok(())
    }

    /// Write a checkpoint entry and fsync.  After this call the caller may
    /// delete the WAL file because all its records are in an SSTable.
    pub fn checkpoint(&mut self) -> Result<()> {
        self.append(ENTRY_CHECKPOINT, &[])?;
        self.sync()
    }

    pub fn path(&self) -> &Path { &self.path }
}

// ── WAL reader / recovery ────────────────────────────────────────────────────

/// A single decoded WAL entry.
#[derive(Debug)]
pub struct WalEntry {
    pub entry_type: u8,
    pub data:       Vec<u8>,
}

/// Replay a WAL file, yielding valid entries up to the first corruption.
/// Stops and returns `Ok` at a `Checkpoint` entry.
pub fn recover(path: impl AsRef<Path>) -> Result<Vec<WalEntry>> {
    let path = path.as_ref();
    if !path.exists() {
        return Ok(vec![]);
    }

    let file  = File::open(path)?;
    let len   = file.metadata()?.len();
    let mut r = BufReader::new(file);
    let mut entries = Vec::new();
    let mut offset: u64 = 0;

    loop {
        if offset >= len { break; }

        // Read header.
        let mut header = [0u8; HEADER_SIZE];
        match r.read_exact(&mut header) {
            Ok(_)  => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                warn!(offset, "WAL truncated in header — stopping recovery");
                break;
            }
            Err(e) => return Err(e.into()),
        }

        let expected_crc = u32::from_le_bytes(header[0..4].try_into().unwrap());
        let data_len     = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
        let entry_type   = header[8];

        // Read data.
        let mut data = vec![0u8; data_len];
        match r.read_exact(&mut data) {
            Ok(_)  => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                warn!(offset, "WAL truncated in data — stopping recovery");
                break;
            }
            Err(e) => return Err(e.into()),
        }

        // Verify CRC.
        let actual_crc = crc32fast::hash(&[&[entry_type], data.as_slice()].concat());
        if actual_crc != expected_crc {
            return Err(StorageError::WalCrcMismatch {
                offset,
                expected: expected_crc,
                actual:   actual_crc,
            });
        }

        offset += (HEADER_SIZE + data_len) as u64;

        if entry_type == ENTRY_CHECKPOINT {
            debug!(offset, "WAL checkpoint found — recovery complete");
            entries.clear(); // All entries before checkpoint are already in SSTable.
            continue;
        }

        entries.push(WalEntry { entry_type, data });
    }

    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn roundtrip_single_entry() {
        let tmp  = NamedTempFile::new().unwrap();
        let path = tmp.path().to_owned();

        let mut wal = Wal::open(&path).unwrap();
        wal.append(ENTRY_LOG, b"hello world").unwrap();
        wal.sync().unwrap();
        drop(wal);

        let entries = recover(&path).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].entry_type, ENTRY_LOG);
        assert_eq!(entries[0].data, b"hello world");
    }

    #[test]
    fn checkpoint_clears_entries() {
        let tmp  = NamedTempFile::new().unwrap();
        let path = tmp.path().to_owned();

        let mut wal = Wal::open(&path).unwrap();
        wal.append(ENTRY_LOG, b"entry1").unwrap();
        wal.append(ENTRY_METRIC, b"entry2").unwrap();
        wal.checkpoint().unwrap();
        wal.append(ENTRY_SPAN, b"entry3").unwrap();
        wal.sync().unwrap();
        drop(wal);

        let entries = recover(&path).unwrap();
        // Only entry3 (after checkpoint) should be returned.
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].entry_type, ENTRY_SPAN);
    }

    #[test]
    fn recovery_survives_empty_file() {
        let tmp = NamedTempFile::new().unwrap();
        let entries = recover(tmp.path()).unwrap();
        assert!(entries.is_empty());
    }
}
