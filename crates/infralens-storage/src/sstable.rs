//! SSTable writer and reader built on Apache Arrow + Parquet.
//!
//! Each flush produces three files:
//!   `{seq:06}.parquet`  — columnar data
//!   `{seq:06}.bloom`    — serialised BloomFilter
//!   `{seq:06}.zonemap`  — serialised ZoneMap

use crate::bloom::BloomFilter;
use crate::error::{Result, StorageError};
use crate::zone_map::ZoneMap;
use arrow::array::{
    ArrayRef, BinaryBuilder, Float64Builder, Int32Builder,
    Int64Builder, Int8Builder, StringBuilder, UInt64Builder,
};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use infralens_common::model::{AnyValue, LogRecord, MetricPoint, MetricType, SignalType, SpanRecord};
use infralens_common::schema::{log_schema, metric_schema, span_schema};
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::debug;

// ── File naming helpers ───────────────────────────────────────────────────────

pub fn parquet_path(dir: &Path, seq: u64) -> PathBuf { dir.join(format!("{seq:06}.parquet")) }
pub fn bloom_path(dir: &Path, seq: u64)   -> PathBuf { dir.join(format!("{seq:06}.bloom"))   }
pub fn zonemap_path(dir: &Path, seq: u64) -> PathBuf { dir.join(format!("{seq:06}.zonemap")) }

// ── SSTable metadata ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct SSTableMeta {
    pub seq:         u64,
    pub signal_type: SignalType,
    pub dir:         PathBuf,
    pub zone_map:    ZoneMap,
}

impl SSTableMeta {
    pub fn parquet_path(&self) -> PathBuf { parquet_path(&self.dir, self.seq) }
    pub fn bloom_path(&self)   -> PathBuf { bloom_path(&self.dir, self.seq)   }
    pub fn zonemap_path(&self) -> PathBuf { zonemap_path(&self.dir, self.seq) }
}

// ── Writer ────────────────────────────────────────────────────────────────────

fn writer_props() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .set_max_row_group_size(1_000_000)
        .build()
}

/// Flush a slice of log records to Parquet + bloom + zone-map files.
pub fn write_logs(
    records:  &[LogRecord],
    dir:      &Path,
    seq:      u64,
) -> Result<SSTableMeta> {
    let schema = log_schema();
    let batch  = build_log_batch(records, schema.clone())?;
    let mut zm = ZoneMap::new();
    let mut bf = BloomFilter::new(records.len().max(1) as u64, 0.02);

    for r in records {
        zm.observe_timestamp(r.timestamp_ns);
        // Bloom key: first 8 bytes of serde_json of severity + body (cheap fingerprint).
        let key = r.severity_text.as_bytes();
        bf.insert(key);
    }

    write_parquet(&batch, &parquet_path(dir, seq))?;
    std::fs::write(bloom_path(dir, seq), bf.to_bytes())?;
    zm.write_to_file(zonemap_path(dir, seq))?;

    debug!(seq, rows = records.len(), "SSTable written (logs)");
    Ok(SSTableMeta { seq, signal_type: SignalType::Log, dir: dir.to_owned(), zone_map: zm })
}

pub fn write_metrics(
    records: &[MetricPoint],
    dir:     &Path,
    seq:     u64,
) -> Result<SSTableMeta> {
    let schema = metric_schema();
    let batch  = build_metric_batch(records, schema.clone())?;
    let mut zm = ZoneMap::new();
    let mut bf = BloomFilter::new(records.len().max(1) as u64, 0.02);

    for r in records {
        zm.observe_timestamp(r.timestamp_ns);
        bf.insert(r.name.as_bytes());
    }

    write_parquet(&batch, &parquet_path(dir, seq))?;
    std::fs::write(bloom_path(dir, seq), bf.to_bytes())?;
    zm.write_to_file(zonemap_path(dir, seq))?;

    debug!(seq, rows = records.len(), "SSTable written (metrics)");
    Ok(SSTableMeta { seq, signal_type: SignalType::Metric, dir: dir.to_owned(), zone_map: zm })
}

pub fn write_spans(
    records: &[SpanRecord],
    dir:     &Path,
    seq:     u64,
) -> Result<SSTableMeta> {
    let schema = span_schema();
    let batch  = build_span_batch(records, schema.clone())?;
    let mut zm = ZoneMap::new();
    let mut bf = BloomFilter::new(records.len().max(1) as u64, 0.02);

    for r in records {
        zm.observe_timestamp(r.start_time_ns);
        bf.insert(&r.trace_id);
    }

    write_parquet(&batch, &parquet_path(dir, seq))?;
    std::fs::write(bloom_path(dir, seq), bf.to_bytes())?;
    zm.write_to_file(zonemap_path(dir, seq))?;

    debug!(seq, rows = records.len(), "SSTable written (spans)");
    Ok(SSTableMeta { seq, signal_type: SignalType::Span, dir: dir.to_owned(), zone_map: zm })
}

fn write_parquet(batch: &RecordBatch, path: &Path) -> Result<()> {
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(writer_props()))?;
    writer.write(batch)?;
    writer.close()?;
    Ok(())
}

// ── Arrow RecordBatch builders ────────────────────────────────────────────────

fn json_opt(v: &Option<AnyValue>) -> Option<String> {
    v.as_ref().and_then(|a| serde_json::to_string(a).ok())
}

fn json_map(m: &std::collections::HashMap<String, AnyValue>) -> Option<String> {
    if m.is_empty() { None } else { serde_json::to_string(m).ok() }
}

fn build_log_batch(records: &[LogRecord], schema: Arc<Schema>) -> Result<RecordBatch> {
    let n = records.len();

    let mut ts_b        = UInt64Builder::with_capacity(n);
    let mut obs_ts_b    = UInt64Builder::with_capacity(n);
    let mut trace_id_b  = BinaryBuilder::new();
    let mut span_id_b   = BinaryBuilder::new();
    let mut sev_num_b   = Int32Builder::with_capacity(n);
    let mut sev_text_b  = StringBuilder::new();
    let mut body_b      = StringBuilder::new();
    let mut attrs_b     = StringBuilder::new();
    let mut res_b       = StringBuilder::new();
    let mut scope_nm_b  = StringBuilder::new();
    let mut scope_ver_b = StringBuilder::new();
    let mut schema_url_b= StringBuilder::new();

    for r in records {
        ts_b.append_value(r.timestamp_ns);
        obs_ts_b.append_value(r.observed_timestamp_ns);
        match &r.trace_id { Some(id) => trace_id_b.append_value(id), None => trace_id_b.append_null() }
        match &r.span_id  { Some(id) => span_id_b.append_value(id),  None => span_id_b.append_null()  }
        sev_num_b.append_value(r.severity_number);
        sev_text_b.append_option(if r.severity_text.is_empty() { None } else { Some(&r.severity_text) });
        body_b.append_option(json_opt(&r.body).as_deref());
        attrs_b.append_option(json_map(&r.attributes).as_deref());
        res_b.append_option(json_map(&r.resource_attributes).as_deref());
        scope_nm_b.append_option(if r.scope.name.is_empty()    { None } else { Some(&r.scope.name) });
        scope_ver_b.append_option(if r.scope.version.is_empty(){ None } else { Some(&r.scope.version) });
        schema_url_b.append_option(if r.schema_url.is_empty()  { None } else { Some(&r.schema_url) });
    }

    let cols: Vec<ArrayRef> = vec![
        Arc::new(ts_b.finish()),
        Arc::new(obs_ts_b.finish()),
        Arc::new(trace_id_b.finish()),
        Arc::new(span_id_b.finish()),
        Arc::new(sev_num_b.finish()),
        Arc::new(sev_text_b.finish()),
        Arc::new(body_b.finish()),
        Arc::new(attrs_b.finish()),
        Arc::new(res_b.finish()),
        Arc::new(scope_nm_b.finish()),
        Arc::new(scope_ver_b.finish()),
        Arc::new(schema_url_b.finish()),
    ];
    Ok(RecordBatch::try_new(schema, cols)?)
}

fn metric_type_id(mt: &MetricType) -> i8 {
    match mt {
        MetricType::Gauge                   => 0,
        MetricType::Sum { .. }              => 1,
        MetricType::Histogram { .. }        => 2,
        MetricType::ExponentialHistogram{..}=> 3,
        MetricType::Summary                 => 4,
    }
}

fn build_metric_batch(records: &[MetricPoint], schema: Arc<Schema>) -> Result<RecordBatch> {
    let n = records.len();

    let mut ts_b        = UInt64Builder::with_capacity(n);
    let mut sts_b       = UInt64Builder::with_capacity(n);
    let mut name_b      = StringBuilder::new();
    let mut desc_b      = StringBuilder::new();
    let mut unit_b      = StringBuilder::new();
    let mut mtype_b     = Int8Builder::with_capacity(n);
    let mut vdbl_b      = Float64Builder::with_capacity(n);
    let mut vint_b      = Int64Builder::with_capacity(n);
    let mut hcount_b    = UInt64Builder::with_capacity(n);
    let mut hsum_b      = Float64Builder::with_capacity(n);
    let mut hbounds_b   = BinaryBuilder::new();
    let mut hcounts_b   = BinaryBuilder::new();
    let mut attrs_b     = StringBuilder::new();
    let mut res_b       = StringBuilder::new();
    let mut scope_nm_b  = StringBuilder::new();
    let mut scope_ver_b = StringBuilder::new();
    let mut schema_url_b= StringBuilder::new();

    for r in records {
        ts_b.append_value(r.timestamp_ns);
        sts_b.append_value(r.start_timestamp_ns);
        name_b.append_value(&r.name);
        desc_b.append_option(if r.description.is_empty() { None } else { Some(&r.description) });
        unit_b.append_option(if r.unit.is_empty()        { None } else { Some(&r.unit) });
        mtype_b.append_value(metric_type_id(&r.metric_type));
        vdbl_b.append_option(r.value_double);
        vint_b.append_option(r.value_int);

        if let Some(h) = &r.histogram {
            hcount_b.append_value(h.count);
            hsum_b.append_option(h.sum);
            let bounds_bytes = bincode::serialize(&h.explicit_bounds)
                .unwrap_or_default();
            let counts_bytes = bincode::serialize(&h.bucket_counts)
                .unwrap_or_default();
            hbounds_b.append_value(&bounds_bytes);
            hcounts_b.append_value(&counts_bytes);
        } else {
            hcount_b.append_null();
            hsum_b.append_null();
            hbounds_b.append_null();
            hcounts_b.append_null();
        }

        attrs_b.append_option(json_map(&r.attributes).as_deref());
        res_b.append_option(json_map(&r.resource_attributes).as_deref());
        scope_nm_b.append_option(if r.scope.name.is_empty()    { None } else { Some(&r.scope.name) });
        scope_ver_b.append_option(if r.scope.version.is_empty(){ None } else { Some(&r.scope.version) });
        schema_url_b.append_option(if r.schema_url.is_empty()  { None } else { Some(&r.schema_url) });
    }

    let cols: Vec<ArrayRef> = vec![
        Arc::new(ts_b.finish()),
        Arc::new(sts_b.finish()),
        Arc::new(name_b.finish()),
        Arc::new(desc_b.finish()),
        Arc::new(unit_b.finish()),
        Arc::new(mtype_b.finish()),
        Arc::new(vdbl_b.finish()),
        Arc::new(vint_b.finish()),
        Arc::new(hcount_b.finish()),
        Arc::new(hsum_b.finish()),
        Arc::new(hbounds_b.finish()),
        Arc::new(hcounts_b.finish()),
        Arc::new(attrs_b.finish()),
        Arc::new(res_b.finish()),
        Arc::new(scope_nm_b.finish()),
        Arc::new(scope_ver_b.finish()),
        Arc::new(schema_url_b.finish()),
    ];
    Ok(RecordBatch::try_new(schema, cols)?)
}

fn build_span_batch(records: &[SpanRecord], schema: Arc<Schema>) -> Result<RecordBatch> {
    let n = records.len();

    let mut trace_id_b    = BinaryBuilder::new();
    let mut span_id_b     = BinaryBuilder::new();
    let mut parent_id_b   = BinaryBuilder::new();
    let mut name_b        = StringBuilder::new();
    let mut kind_b        = Int32Builder::with_capacity(n);
    let mut start_b       = UInt64Builder::with_capacity(n);
    let mut end_b         = UInt64Builder::with_capacity(n);
    let mut dur_b         = UInt64Builder::with_capacity(n);
    let mut status_code_b = Int32Builder::with_capacity(n);
    let mut status_msg_b  = StringBuilder::new();
    let mut attrs_b       = StringBuilder::new();
    let mut res_b         = StringBuilder::new();
    let mut scope_nm_b    = StringBuilder::new();
    let mut scope_ver_b   = StringBuilder::new();
    let mut events_b      = StringBuilder::new();
    let mut links_b       = StringBuilder::new();
    let mut schema_url_b  = StringBuilder::new();

    for r in records {
        trace_id_b.append_value(&r.trace_id);
        span_id_b.append_value(&r.span_id);
        match &r.parent_span_id {
            Some(id) => parent_id_b.append_value(id.as_ref()),
            None     => parent_id_b.append_null(),
        }
        name_b.append_value(&r.name);
        kind_b.append_value(r.kind);
        start_b.append_value(r.start_time_ns);
        end_b.append_value(r.end_time_ns);
        dur_b.append_value(r.duration_ns);
        status_code_b.append_value(r.status_code);
        status_msg_b.append_option(if r.status_message.is_empty() { None } else { Some(&r.status_message) });
        attrs_b.append_option(json_map(&r.attributes).as_deref());
        res_b.append_option(json_map(&r.resource_attributes).as_deref());
        scope_nm_b.append_option(if r.scope.name.is_empty()    { None } else { Some(&r.scope.name) });
        scope_ver_b.append_option(if r.scope.version.is_empty(){ None } else { Some(&r.scope.version) });
        let events_json = serde_json::to_string(&r.events).ok();
        events_b.append_option(events_json.as_deref());
        let links_json = serde_json::to_string(&r.links).ok();
        links_b.append_option(links_json.as_deref());
        schema_url_b.append_option(if r.schema_url.is_empty() { None } else { Some(&r.schema_url) });
    }

    let cols: Vec<ArrayRef> = vec![
        Arc::new(trace_id_b.finish()),
        Arc::new(span_id_b.finish()),
        Arc::new(parent_id_b.finish()),
        Arc::new(name_b.finish()),
        Arc::new(kind_b.finish()),
        Arc::new(start_b.finish()),
        Arc::new(end_b.finish()),
        Arc::new(dur_b.finish()),
        Arc::new(status_code_b.finish()),
        Arc::new(status_msg_b.finish()),
        Arc::new(attrs_b.finish()),
        Arc::new(res_b.finish()),
        Arc::new(scope_nm_b.finish()),
        Arc::new(scope_ver_b.finish()),
        Arc::new(events_b.finish()),
        Arc::new(links_b.finish()),
        Arc::new(schema_url_b.finish()),
    ];
    Ok(RecordBatch::try_new(schema, cols)?)
}

// ── Reader ────────────────────────────────────────────────────────────────────

/// Open a Parquet SSTable and return an iterator of RecordBatches.
pub fn read_parquet(
    path: &Path,
    batch_size: usize,
) -> Result<impl Iterator<Item = Result<RecordBatch>>> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?
        .with_batch_size(batch_size);
    let reader = builder.build()?;
    Ok(reader.map(|r| r.map_err(StorageError::from)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use infralens_common::model::{AnyValue, InstrumentationScope, LogRecord};
    use std::collections::HashMap;
    use tempfile::TempDir;

    fn make_log(ts: u64) -> LogRecord {
        LogRecord {
            timestamp_ns:          ts,
            observed_timestamp_ns: ts,
            trace_id:              Some([0u8; 16]),
            span_id:               Some([0u8; 8]),
            severity_number:       9,
            severity_text:         "INFO".into(),
            body:                  Some(AnyValue::String("hello".into())),
            attributes:            [("key".into(), AnyValue::Int(42))].into(),
            resource_attributes:   HashMap::new(),
            scope:                 InstrumentationScope::default(),
            schema_url:            String::new(),
        }
    }

    #[test]
    fn write_then_read_logs() {
        let dir = TempDir::new().unwrap();
        let records: Vec<_> = (0..100).map(|i| make_log(i * 1_000_000)).collect();
        let meta = write_logs(&records, dir.path(), 1).unwrap();

        assert!(meta.parquet_path().exists());
        assert!(meta.bloom_path().exists());
        assert!(meta.zonemap_path().exists());

        let batches: Vec<RecordBatch> = read_parquet(&meta.parquet_path(), 1024)
            .unwrap()
            .collect::<Result<_>>()
            .unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 100);
    }

    #[test]
    fn zone_map_time_range() {
        let dir = TempDir::new().unwrap();
        let records: Vec<_> = (1..=10).map(|i| make_log(i * 1_000_000_000)).collect();
        let meta = write_logs(&records, dir.path(), 2).unwrap();

        assert_eq!(meta.zone_map.min_timestamp_ns, 1_000_000_000);
        assert_eq!(meta.zone_map.max_timestamp_ns, 10_000_000_000);
        assert!(meta.zone_map.overlaps_time_range(5_000_000_000, 15_000_000_000));
        assert!(!meta.zone_map.overlaps_time_range(0, 500_000_000));
    }
}
