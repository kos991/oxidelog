# ClickHouse + DuckDB 混合架构设计

## 架构概览

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              中心层 (ClickHouse)                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  events (MergeTree)                                                 │   │
│  │  ├─ PARTITION BY toYYYYMMDD(ingest_time)                           │   │
│  │  ├─ ORDER BY (ingest_time, source_addr, protocol)                  │   │
│  │  └─ TTL toDate(ingest_time) + 90 DAY -> S3                         │   │
│  │                                                                     │   │
│  │  mv_minute_metrics (SummingMergeTree)  ← 物化视图自动聚合            │   │
│  │  mv_source_metrics (SummingMergeTree)                               │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    ▲                                        │
│                    async 批量写入   │   SQL 查询                             │
│                                    │                                        │
└────────────────────────────────────┼────────────────────────────────────────┘
                                     │
                      ┌──────────────┼──────────────┐
                      │              │              │
                      ▼              ▼              ▼
┌─────────────────────────┐  ┌─────────────────────────┐  ┌─────────────────┐
│     边缘节点 A           │  │     边缘节点 B           │  │   开发/运维工作站  │
│  ┌───────────────────┐  │  │  ┌───────────────────┐  │  │                 │
│  │   fwlogd          │  │  │  │   fwlogd          │  │  │  fwlog-import   │
│  │   ├─ TCP/UDP 接收  │  │  │  │   ├─ TCP/UDP 接收  │  │  │                 │
│  │   ├─ 解析引擎       │  │  │  │   ├─ 解析引擎       │  │  │  DuckDB 本地    │
│  │   ├─ DuckDB 热库   │  │  │  │   ├─ DuckDB 热库   │  │  │  预处理/探索分析  │
│  │   │  (最近1小时)    │  │  │  │   │  (最近1小时)    │  │  │                 │
│  │   └─ API (本地排查) │  │  │  │   └─ API (本地排查) │  │  │                 │
│  └───────────────────┘  │  │  └───────────────────┘  │  │                 │
└─────────────────────────┘  └─────────────────────────┘  └─────────────────┘
```

## 职责分工

| 场景 | DuckDB（边缘） | ClickHouse（中心） |
|------|---------------|-------------------|
| 实时写入 | 本地缓存最近 1 小时 | 全量历史数据 |
| 实时告警 | 本地规则引擎 + 本地查询 | 全局关联分析 |
| 故障排查 | 毫秒级查询最近日志 | 跨节点/跨时段深度分析 |
| 大盘指标 | 本地分钟级缓存 | 全局物化视图 |
| 离线导入 | CSV/Parquet 预处理 | 最终存储 |
| 开发探索 | 本地采样数据快速验证 | 生产全量查询 |
| 高并发查询 | 单用户本地 | 多用户并发 |

## 数据流设计

### 1. 实时写入：双写策略

```rust
// crates/fwlog-storage/src/hybrid.rs
pub struct HybridStorage {
    local: DuckDbStore,           // 边缘热缓存
    remote: ClickHouseClient,     // 中心主存储
    remote_enabled: AtomicBool,
}

impl HybridStorage {
    pub fn insert_batch(&self, events: &[CanonicalEvent]) -> Result<usize> {
        // 1. 本地 DuckDB 始终写入（兜底 + 本地查询）
        let local_inserted = self.local.insert_batch(events)?;
        
        // 2. 异步写入 ClickHouse（不阻塞主流程）
        if self.remote_enabled.load(Ordering::Relaxed) {
            let events = events.to_vec();
            let client = self.remote.clone();
            tokio::spawn(async move {
                if let Err(err) = client.insert_batch(&events).await {
                    error!(error = %err, "clickhouse write failed");
                }
            });
        }
        
        Ok(local_inserted)
    }
}
```

**关键点：** ClickHouse 写入失败不影响本地流程，系统具备降级能力。

### 2. ClickHouse 表结构

```sql
-- 主表：承接 DuckDB 的 events
CREATE TABLE events (
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
    raw String CODEC(ZSTD(3)),
    parse_status LowCardinality(String),
    parse_error String CODEC(ZSTD(3))
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(ingest_time)
ORDER BY (ingest_time, source_addr, protocol, action)
TTL toDate(ingest_time) + INTERVAL 90 DAY 
    TO VOLUME 'cold';

-- 物化视图：完全替代应用层 nat_minute_metrics
CREATE MATERIALIZED VIEW mv_minute_metrics
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMMDD(bucket_minute)
ORDER BY (bucket_minute, source_addr, protocol, action, parse_status)
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

-- 物化视图：源维度指标
CREATE MATERIALIZED VIEW mv_source_metrics
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMMDD(bucket_hour)
ORDER BY (bucket_hour, source_addr)
AS SELECT
    toStartOfHour(ingest_time) AS bucket_hour,
    source_addr,
    count() AS total_count,
    sum(length(raw)) AS raw_bytes,
    max(ingest_time) AS last_seen
FROM events
GROUP BY bucket_hour, source_addr;
```

### 3. 查询路由策略

```rust
// crates/fwlog-api/src/search.rs
pub enum QueryTarget {
    Local,      // DuckDB：最近 1 小时、本地排查
    Remote,     // ClickHouse：历史查询、大盘指标
    Auto,       // 自动路由：时间范围决定
}

impl HybridSearchBackend {
    fn route(&self, query: &EventQuery) -> QueryTarget {
        // 最近 1 小时且简单查询 -> DuckDB
        if query.within_last_hours(1) && !query.is_aggregate() {
            return QueryTarget::Local;
        }
        
        // 历史数据或聚合查询 -> ClickHouse
        QueryTarget::Remote
    }
    
    pub fn search(&self, query: &EventQuery, limit: usize) -> Result<Vec<CanonicalEvent>> {
        match self.route(query) {
            QueryTarget::Local => {
                let store = DuckDbStore::open_read_only(&self.local_path)?;
                store.query_events(query, limit)
            }
            QueryTarget::Remote => {
                self.ch_client.query_events(query, limit).block_on()
            }
            QueryTarget::Auto => {
                // 智能路由逻辑
                if query.within_last_hours(1) && !query.is_aggregate() {
                    self.search_local(query, limit)
                } else {
                    self.search_remote(query, limit)
                }
            }
        }
    }
}
```

### 4. 离线导入：DuckDB 预处理 → ClickHouse 入库

```bash
# 1. 本地 DuckDB 快速解析和清洗
fwlog-import --input /var/log/firewall/ --duckdb /tmp/staging.duckdb

# 2. 导出为 ClickHouse 原生格式
# DuckDB 直接生成 ClickHouse 可读的 Parquet
DuckDbStore::open("/tmp/staging.duckdb")?
    .export_parquet_for_clickhouse("/tmp/batch.parquet")?;

# 3. ClickHouse 批量加载
clickhouse-client --query "INSERT INTO events FORMAT Parquet" < /tmp/batch.parquet
```

**为什么用 DuckDB 做预处理？**
- 本地解析速度极快（列式 + Appender）
- 可以对原始日志做采样、去重、格式验证
- 网络中断时本地不丢数据，攒批后统一上传

## Rust 集成代码

### ClickHouse 客户端封装

```rust
// crates/fwlog-storage/Cargo.toml
[dependencies]
clickhouse = "0.13"

// crates/fwlog-storage/src/clickhouse.rs
use clickhouse::{Client, Row};
use serde::Serialize;

pub struct ClickHouseStorage {
    client: Client,
}

impl ClickHouseStorage {
    pub fn new(url: &str, database: &str) -> Result<Self> {
        let client = Client::default()
            .with_url(url)
            .with_database(database)
            .with_compression(clickhouse::Compression::Lz4);
        Ok(Self { client })
    }

    pub async fn insert_batch(&self, events: &[CanonicalEvent]) -> Result<usize> {
        let mut insert = self.client.insert("events")?;
        for event in events {
            insert.write(&ClickHouseEvent::from(event)).await?;
        }
        insert.end().await?;
        Ok(events.len())
    }
    
    pub async fn query_events(
        &self,
        query: &EventQuery,
        limit: usize,
    ) -> Result<Vec<CanonicalEvent>> {
        let sql = build_query_sql(query, limit);
        let rows = self.client
            .query(&sql)
            .fetch_all::<ClickHouseEvent>()
            .await?;
        Ok(rows.into_iter().map(|r| r.into()).collect())
    }
}

#[derive(Row, Serialize)]
struct ClickHouseEvent {
    event_id: String,
    ingest_time: chrono::DateTime<chrono::Utc>,
    source_addr: String,
    device_id: Option<String>,
    event_time: Option<chrono::DateTime<chrono::Utc>>,
    vendor: Option<String>,
    product: Option<String>,
    src_ip: Option<String>,
    src_port: Option<u16>,
    dst_ip: Option<String>,
    dst_port: Option<u16>,
    protocol: Option<String>,
    action: Option<String>,
    severity: Option<String>,
    raw: String,
    parse_status: String,
    parse_error: Option<String>,
}
```

## 部署拓扑

### 小规模（单节点）

```yaml
# docker-compose.yml
services:
  fwlogd:
    image: oxidelog/fwlogd
    volumes:
      - ./data:/data
    environment:
      - STORAGE_MODE=hybrid
      - CLICKHOUSE_URL=http://clickhouse:8123
    ports:
      - "1514:1514/udp"
      - "18080:18080"

  clickhouse:
    image: clickhouse/clickhouse-server
    volumes:
      - ./ch_data:/var/lib/clickhouse
```

### 中大规模（多采集节点）

```
                ┌──────────────┐
                │   Nginx      │
                │  (API 网关)   │
                └──────┬───────┘
                       │
        ┌──────────────┼──────────────┐
        ▼              ▼              ▼
   ┌─────────┐   ┌─────────┐   ┌─────────┐
   │ fwlogd  │   │ fwlogd  │   │ fwlogd  │
   │ 节点-A  │   │ 节点-B  │   │ 节点-C  │
   │ DuckDB  │   │ DuckDB  │   │ DuckDB  │
   └────┬────┘   └────┬────┘   └────┬────┘
        │             │             │
        └─────────────┼─────────────┘
                      │ async 写入
                      ▼
              ┌───────────────┐
              │  ClickHouse   │
              │   Cluster     │
              └───────────────┘
```

- 每个机房/区域部署一个 fwlogd + DuckDB 节点，本地快速查询
- 所有节点异步汇聚到中心 ClickHouse
- API 网关根据查询时间范围路由到本地或中心

## 迁移路径

### 阶段 1：并行期（2 周）
- 部署 ClickHouse
- fwlogd 开启双写（DuckDB + ClickHouse）
- 前端默认查询 DuckDB，大盘查询可选 ClickHouse
- 验证数据一致性

### 阶段 2：切换期（1 周）
- 分钟/小时级指标查询切到 ClickHouse 物化视图
- 历史搜索（>1天）切到 ClickHouse
- DuckDB 仅保留本地最近 1 小时缓存

### 阶段 3：优化期（持续）
- DuckDB 用于离线导入预处理
- ClickHouse TTL 自动归档到 S3
- 边缘节点独立部署

## 配置示例

```toml
# config/server.toml

[storage]
mode = "hybrid"  # "local" | "hybrid" | "clickhouse"

[storage.local]
duckdb_path = "data/duckdb/oxidelog.duckdb"
retention_hours = 1  # 仅保留最近 1 小时

[storage.clickhouse]
enabled = true
url = "http://clickhouse:8123"
database = "oxidelog"
username = "default"
password = ""
batch_size = 10000
flush_interval_ms = 5000
```

## 何时引入 ClickHouse

**当前阶段（纯DuckDB）适合：**
- 单机部署场景
- 日志量 < 1TB
- 并发查询 < 10
- 团队规模小

**引入 ClickHouse 的信号：**
- 多节点采集需求
- 历史数据 > 500GB
- 并发查询频繁卡顿（即使双库轮转后）
- 需要跨节点全局分析
- 需要复杂的 OLAP 分析

## 性能对比

| 指标 | 纯 DuckDB | DuckDB + ClickHouse |
|------|-----------|---------------------|
| 写入吞吐 | 50k events/s | 50k events/s (本地) + 异步同步 |
| 查询延迟（最近1小时） | <100ms | <100ms (DuckDB) |
| 查询延迟（历史数据） | 1-10s | <500ms (ClickHouse) |
| 并发查询 | 10 | 100+ |
| 存储成本 | 本地磁盘 | 本地 + S3 (冷数据) |
| 运维复杂度 | 低 | 中 |

## 总结

DuckDB 是 OxideLog 的"边缘大脑"（快速、嵌入、本地），ClickHouse 是"中心仓库"（海量、高并发、全局）。两者通过"双写 + 智能路由"结合，既保留了单机部署的简洁性，又获得了分布式分析的扩展能力。

**核心原则：**
- 先优化 DuckDB（已完成）
- 观察性能瓶颈
- 按需引入 ClickHouse
- 保持架构灵活性
