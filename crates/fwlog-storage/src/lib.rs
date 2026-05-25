mod archive;
mod clickhouse;
mod dual_db;
mod duckdb;
mod frozen;
mod governor;
mod hybrid;
mod lifecycle;

use anyhow::Result;
use chrono::{DateTime, Utc};

pub use archive::{list_archive_files, prune_archive_files, ArchiveFile};
pub use clickhouse::ClickHouseStorage;
pub use dual_db::{DualDbConfig, DualDbManager, DualDbMetrics, SyncReport};
pub use duckdb::{
    AdaptiveFieldRuleCheckpointRow, AdaptiveFieldRuleRow, DeviceBinding, DuckDbStore, EventQuery,
    FrozenArchiveIndex, IpRegionCacheEntry, MinuteMetricPoint, MinuteMetricQuery,
    ParserAdaptiveCheckpoint, ParserCheckpointVersionRow, ParserDiagnosticCheckpointRow,
    ParserDiagnosticRow, ParserProfileCheckpointRow, ParserProfileRow, ParserScopeCheckpointRow,
    ParserScopeRow, SourceDeviceAliasCheckpointRow, SourceDeviceAliasRow, SourceMetricBucket,
    SourceMetricQuery,
};
pub use frozen::{
    list_frozen_files, prune_frozen_files, read_frozen_raw, write_frozen_raw, FrozenFile,
};
pub use governor::{
    run_storage_governor, ArchiveConfig as GovernorArchiveConfig,
    GovernorConfig, GovernorCycleReport, LifecycleConfig as GovernorLifecycleConfig,
};
pub use hybrid::{HybridConfig, HybridHealth, HybridStats, HybridStorage};
pub use lifecycle::{run_lifecycle_to, LifecycleReport};

/// Internal utility for date parsing across storage modules
pub(crate) fn parse_any_date(s: &str) -> Result<DateTime<Utc>> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.with_timezone(&Utc));
    }
    if let Ok(d) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return Ok(DateTime::<Utc>::from_naive_utc_and_offset(
            d.and_hms_opt(0, 0, 0).unwrap(),
            Utc,
        ));
    }
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return Ok(DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc));
    }
    anyhow::bail!("invalid date format: {}", s)
}
