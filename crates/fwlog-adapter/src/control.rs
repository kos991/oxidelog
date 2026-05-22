use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use fwlog_domain::ParseStatus;
use serde::{Deserialize, Serialize};

const DEFAULT_MAX_PROFILE_DELTAS: usize = 4096;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParserProfileDelta {
    pub scope_key: String,
    pub parser_id: String,
    pub status: ParseStatus,
    pub count: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetricsFlushEvent {
    pub profile_deltas: Vec<ParserProfileDelta>,
    pub dropped_batches: u64,
    pub scope_gaps: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParserScopeState {
    pub scope_key: String,
    pub source_high_entropy: bool,
    pub adaptive_learning_enabled: bool,
    pub unknown_source_bucket: bool,
    pub metrics_gap: bool,
    pub metrics_gap_since: Option<DateTime<Utc>>,
    pub malformed_flood_until: Option<DateTime<Utc>>,
    pub adaptive_quarantine_until: Option<DateTime<Utc>>,
    pub quarantine_backoff: u32,
    pub last_state_change: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
}

impl ParserScopeState {
    pub fn new(scope_key: impl Into<String>, now: DateTime<Utc>) -> Self {
        Self {
            scope_key: scope_key.into(),
            source_high_entropy: false,
            adaptive_learning_enabled: true,
            unknown_source_bucket: false,
            metrics_gap: false,
            metrics_gap_since: None,
            malformed_flood_until: None,
            adaptive_quarantine_until: None,
            quarantine_backoff: 0,
            last_state_change: now,
            last_seen: now,
        }
    }

    pub fn unknown_bucket(scope_key: impl Into<String>, now: DateTime<Utc>) -> Self {
        let mut state = Self::new(scope_key, now);
        state.unknown_source_bucket = true;
        state.adaptive_learning_enabled = false;
        state
    }

    pub fn mark_metrics_gap(&mut self, now: DateTime<Utc>) {
        self.metrics_gap = true;
        self.metrics_gap_since.get_or_insert(now);
        self.last_state_change = now;
        self.last_seen = now;
    }
}

#[derive(Debug, Clone)]
pub struct LocalParserMetricsBatch {
    profile_deltas: Vec<ParserProfileDelta>,
    max_entries: usize,
    pub dropped_batches: u64,
    pub scope_gaps: BTreeSet<String>,
}

impl Default for LocalParserMetricsBatch {
    fn default() -> Self {
        Self::with_max_entries(DEFAULT_MAX_PROFILE_DELTAS)
    }
}

impl LocalParserMetricsBatch {
    pub fn with_max_entries(max_entries: usize) -> Self {
        Self {
            profile_deltas: Vec::new(),
            max_entries,
            dropped_batches: 0,
            scope_gaps: BTreeSet::new(),
        }
    }

    pub fn record(&mut self, scope_key: &str, parser_id: &str, status: ParseStatus) {
        if let Some(delta) = self.profile_deltas.iter_mut().find(|delta| {
            delta.scope_key == scope_key && delta.parser_id == parser_id && delta.status == status
        }) {
            delta.count = delta.count.saturating_add(1);
            return;
        }

        if self.profile_deltas.len() >= self.max_entries {
            self.dropped_batches = self.dropped_batches.saturating_add(1);
            self.scope_gaps.insert(scope_key.to_string());
            return;
        }

        self.profile_deltas.push(ParserProfileDelta {
            scope_key: scope_key.to_string(),
            parser_id: parser_id.to_string(),
            status,
            count: 1,
        });
    }

    pub fn flush(&mut self) -> MetricsFlushEvent {
        let profile_deltas = std::mem::take(&mut self.profile_deltas);
        let dropped_batches = std::mem::take(&mut self.dropped_batches);
        let scope_gaps = std::mem::take(&mut self.scope_gaps).into_iter().collect();

        MetricsFlushEvent {
            profile_deltas,
            dropped_batches,
            scope_gaps,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.profile_deltas.is_empty() && self.dropped_batches == 0 && self.scope_gaps.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fwlog_domain::ParseStatus;

    #[test]
    fn local_batch_aggregates_by_scope_parser_and_status() {
        let mut batch = LocalParserMetricsBatch::default();
        batch.record(
            "source:udp://192.168.1.10",
            "parser:sangfor_nat_v1",
            ParseStatus::Parsed,
        );
        batch.record(
            "source:udp://192.168.1.10",
            "parser:sangfor_nat_v1",
            ParseStatus::Parsed,
        );
        batch.record(
            "source:udp://192.168.1.10",
            "parser:sangfor_nat_v1",
            ParseStatus::Partial,
        );

        let flush = batch.flush();
        assert_eq!(flush.profile_deltas.len(), 2);
        assert!(flush.profile_deltas.iter().any(|delta| {
            delta.scope_key == "source:udp://192.168.1.10"
                && delta.parser_id == "parser:sangfor_nat_v1"
                && delta.status == ParseStatus::Parsed
                && delta.count == 2
        }));
    }

    #[test]
    fn overflow_marks_metrics_gap() {
        let mut batch = LocalParserMetricsBatch::with_max_entries(1);
        batch.record("source:a", "parser:a", ParseStatus::Parsed);
        batch.record("source:b", "parser:b", ParseStatus::Failed);

        assert_eq!(batch.dropped_batches, 1);
        assert!(batch.scope_gaps.contains(&"source:b".to_string()));
    }
}
