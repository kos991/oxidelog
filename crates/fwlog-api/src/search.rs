use std::path::PathBuf;

use anyhow::Result;
use fwlog_domain::CanonicalEvent;
use fwlog_storage::{DuckDbStore, EventQuery};

pub trait SearchBackend {
    fn search(&self, query: &EventQuery, limit: usize) -> Result<Vec<CanonicalEvent>>;
}

#[derive(Debug, Clone)]
pub struct NativeSearchBackend {
    duckdb_path: PathBuf,
}

impl NativeSearchBackend {
    pub fn new(duckdb_path: PathBuf) -> Self {
        Self { duckdb_path }
    }
}

impl SearchBackend for NativeSearchBackend {
    fn search(&self, query: &EventQuery, limit: usize) -> Result<Vec<CanonicalEvent>> {
        let store = DuckDbStore::open_read_only(&self.duckdb_path)?;
        if query == &EventQuery::default() {
            store.query_recent(limit)
        } else {
            store.query_events(query, limit)
        }
    }
}
