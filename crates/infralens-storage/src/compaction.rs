//! Background compaction worker.
//!
//! Phase 1 strategy: when L0 file count exceeds `l0_compaction_trigger`, merge
//! all L0 files into a single new SSTable.  The old files are deleted after the
//! new file is fsync'd.

use crate::bloom::BloomFilter;
use crate::error::Result;
use crate::partition::Partition;
use crate::sstable::{read_parquet, SSTableMeta};
use crate::zone_map::ZoneMap;
use arrow::array::UInt64Array;
use arrow::record_batch::RecordBatch;
use infralens_common::model::SignalType;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::fs;
use tracing::{debug, info, warn};

/// Run one compaction pass over all signal sub-partitions.
///
/// Returns the number of compaction operations performed.
pub async fn compact_partition(partition: &Partition, l0_trigger: usize) -> usize {
    let mut ops = 0;
    for signal in [SignalType::Log, SignalType::Metric, SignalType::Span] {
        let sp = partition.signal_partition(signal);
        if sp.sstable_count() >= l0_trigger {
            match compact_signal(sp, signal) {
                Ok(true)  => { ops += 1; }
                Ok(false) => {}
                Err(e)    => warn!(error = %e, ?signal, "compaction error"),
            }
        }
    }
    ops
}

/// Merge all SSTables in a signal sub-partition into one.
/// Returns `Ok(true)` if a merge was performed, `Ok(false)` if skipped.
fn compact_signal(
    sp:     &crate::partition::SignalPartition,
    signal: SignalType,
) -> Result<bool> {
    let mut guard = sp.sstables.lock();
    if guard.len() < 2 { return Ok(false); }

    let inputs: Vec<SSTableMeta> = guard.drain(..).collect();
    drop(guard);

    let output_seq = sp.next_seq();
    info!(
        signal = ?signal,
        input_count = inputs.len(),
        output_seq,
        "Starting compaction"
    );

    // Read all input batches.
    let mut all_batches: Vec<RecordBatch> = Vec::new();
    for meta in &inputs {
        let path = meta.parquet_path();
        if !path.exists() { continue; }
        match read_parquet(&path, 65_536) {
            Ok(iter) => {
                for batch in iter {
                    match batch {
                        Ok(b)  => all_batches.push(b),
                        Err(e) => warn!(error = %e, "skipping bad batch during compaction"),
                    }
                }
            }
            Err(e) => warn!(error = %e, path = %path.display(), "cannot open SSTable"),
        }
    }

    if all_batches.is_empty() {
        // Nothing to compact; clean up empty inputs and return.
        delete_sstable_files(&inputs);
        return Ok(false);
    }

    // Determine schema from first batch.
    let schema = all_batches[0].schema();

    // Write merged output.
    let output_parquet = crate::sstable::parquet_path(&inputs[0].dir, output_seq);
    let output_bloom   = crate::sstable::bloom_path(&inputs[0].dir, output_seq);
    let output_zonemap = crate::sstable::zonemap_path(&inputs[0].dir, output_seq);

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .set_max_row_group_size(1_000_000)
        .build();

    let file   = fs::File::create(&output_parquet)?;
    let mut w  = ArrowWriter::try_new(file, schema, Some(props))?;
    let mut zm = ZoneMap::new();
    let mut bf = BloomFilter::new(all_batches.iter().map(|b| b.num_rows()).sum::<usize>().max(1) as u64, 0.02);

    for batch in &all_batches {
        w.write(batch)?;
        // Update zone map using first column (always timestamp_ns).
        if let Some(col) = batch.column_by_name("timestamp_ns")
            .or_else(|| batch.column_by_name("start_time_ns"))
        {
            if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
                for v in arr.values() { zm.observe_timestamp(*v); }
            }
        }
    }
    w.close()?;

    // fsync
    {
        let f = fs::OpenOptions::new().write(true).open(&output_parquet)?;
        f.sync_data()?;
    }

    fs::write(&output_bloom, bf.to_bytes())?;
    zm.write_to_file(&output_zonemap)?;

    // Register the new SSTable.
    let output_meta = SSTableMeta {
        seq:         output_seq,
        signal_type: signal,
        dir:         inputs[0].dir.clone(),
        zone_map:    zm,
    };
    sp.add_sstable(output_meta);

    // Delete old files.
    delete_sstable_files(&inputs);

    info!(output_seq, "Compaction complete");
    Ok(true)
}

fn delete_sstable_files(metas: &[SSTableMeta]) {
    for m in metas {
        for path in [m.parquet_path(), m.bloom_path(), m.zonemap_path()] {
            if let Err(e) = fs::remove_file(&path) {
                debug!(error = %e, path = %path.display(), "could not delete old SSTable file");
            }
        }
    }
}
