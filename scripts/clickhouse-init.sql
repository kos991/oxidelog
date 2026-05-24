-- OxideLog ClickHouse Schema
-- 优化目标：10-100x 压缩率，快速历史查询

CREATE DATABASE IF NOT EXISTS oxidelog;

USE oxidelog;

-- 主表：日志事件
CREATE TABLE IF NOT EXISTS events (
    event_id String,
    ingest_time DateTime64(3),
    source_addr LowCardinality(String),
    device_id LowCardinality(String),
    event_time DateTime64(3),
    vendor LowCardinality(String),
    product LowCardinality(String),
    src_ip IPv4,
    src_port UInt16,
    dst_ip IPv4,
    dst_port UInt16,
    protocol LowCardinality(String),
    action LowCardinality(String),
    severity LowCardinality(String),
    raw String CODEC(ZSTD(3)),           -- ZSTD 压缩原始日志
    parse_status LowCardinality(String),
    parse_error String CODEC(ZSTD(3))
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(ingest_time)    -- 按天分区，便于删除旧数据
ORDER BY (ingest_time, source_addr, protocol, action)
TTL toDate(ingest_time) + INTERVAL 90 DAY DELETE  -- 90天后自动删除
SETTINGS
    index_granularity = 8192,
    storage_policy = 'default';

-- 物化视图：分钟级指标（替代应用层 nat_minute_metrics）
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_minute_metrics
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
FROM events
GROUP BY bucket_minute, source_addr, protocol, action, parse_status;

-- 物化视图：源设备小时级指标
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_source_metrics
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
FROM events
GROUP BY bucket_hour, source_addr;

-- 物化视图：协议分布（用于大盘）
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_protocol_stats
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMMDD(bucket_hour)
ORDER BY (bucket_hour, protocol, action)
TTL toDate(bucket_hour) + INTERVAL 30 DAY DELETE
AS SELECT
    toStartOfHour(ingest_time) AS bucket_hour,
    protocol,
    action,
    count() AS total_count
FROM events
GROUP BY bucket_hour, protocol, action;

-- 索引：加速 IP 查询
ALTER TABLE events ADD INDEX idx_src_ip src_ip TYPE minmax GRANULARITY 4;
ALTER TABLE events ADD INDEX idx_dst_ip dst_ip TYPE minmax GRANULARITY 4;

-- 查询示例（验证用）
-- SELECT count() FROM events;
-- SELECT bucket_minute, sum(total_count) FROM mv_minute_metrics GROUP BY bucket_minute ORDER BY bucket_minute DESC LIMIT 10;
