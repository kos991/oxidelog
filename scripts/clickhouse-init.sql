CREATE DATABASE IF NOT EXISTS oxidelog;

CREATE TABLE IF NOT EXISTS oxidelog.events (
    event_id String,
    ingest_time DateTime64(3, 'UTC'),
    source_addr LowCardinality(String),
    device_id LowCardinality(String),
    event_time DateTime64(3, 'UTC'),
    vendor LowCardinality(String),
    product LowCardinality(String),
    src_ip String,
    src_port UInt16,
    dst_ip String,
    dst_port UInt16,
    protocol LowCardinality(String),
    action LowCardinality(String),
    severity LowCardinality(String),
    raw String CODEC(ZSTD(3)),
    parse_status LowCardinality(String),
    parse_error String CODEC(ZSTD(3))
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(ingest_time)
ORDER BY (ingest_time, source_addr, protocol, action)
TTL toDate(ingest_time) + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS oxidelog.mv_minute_metrics
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMMDD(bucket_minute)
ORDER BY (bucket_minute, source_addr, protocol, action, parse_status)
TTL toDate(bucket_minute) + INTERVAL 30 DAY DELETE
AS SELECT
    toStartOfMinute(ingest_time) AS bucket_minute,
    source_addr,
    protocol,
    action,
    parse_status,
    count() AS total_count,
    sum(length(raw)) AS raw_bytes
FROM oxidelog.events
GROUP BY bucket_minute, source_addr, protocol, action, parse_status;

CREATE MATERIALIZED VIEW IF NOT EXISTS oxidelog.mv_source_metrics
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMMDD(bucket_hour)
ORDER BY (bucket_hour, source_addr)
TTL toDate(bucket_hour) + INTERVAL 30 DAY DELETE
AS SELECT
    toStartOfHour(ingest_time) AS bucket_hour,
    source_addr,
    count() AS total_count,
    sum(length(raw)) AS raw_bytes,
    max(ingest_time) AS last_seen
FROM oxidelog.events
GROUP BY bucket_hour, source_addr;

CREATE MATERIALIZED VIEW IF NOT EXISTS oxidelog.mv_protocol_stats
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMMDD(bucket_hour)
ORDER BY (bucket_hour, protocol, action)
TTL toDate(bucket_hour) + INTERVAL 30 DAY DELETE
AS SELECT
    toStartOfHour(ingest_time) AS bucket_hour,
    protocol,
    action,
    count() AS total_count
FROM oxidelog.events
GROUP BY bucket_hour, protocol, action;

ALTER TABLE oxidelog.events
    ADD INDEX IF NOT EXISTS idx_src_ip src_ip TYPE tokenbf_v1(2048, 3, 0) GRANULARITY 4;

ALTER TABLE oxidelog.events
    ADD INDEX IF NOT EXISTS idx_dst_ip dst_ip TYPE tokenbf_v1(2048, 3, 0) GRANULARITY 4;
