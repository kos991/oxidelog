# OxideLog 测试计划

> **项目状态**: 主代码已构建完成，查询逻辑正常  
> **现有测试**: 143 个单元/集成测试（跨 16 个源文件）  
> **目标**: 覆盖新模块、验证 ClickHouse 兼容性、保障生产部署质量

---

## ⚡ 测试执行规则

根据项目惯例，测试应通过**目标服务器/部署路径**执行，不在本地 Windows 工作站上运行 `cargo test`、`cargo build` 等命令。以下两种情况例外：

1. **ClickHouse 集成测试** — 通过 Docker Compose 在目标服务器上启动 ClickHouse 后运行
2. **生产冒烟测试** — 通过 `scripts/smoke-production.sh` 对已部署服务进行 HTTP 验证

详细规则参见项目 `oxidelog-server-validation` SKILL 文档。

---

## 一、测试覆盖现状

| 模块 | 文件 | 单元测试数 | 集成测试数 | 覆盖状态 |
|------|------|-----------|-----------|---------|
| **DuckDbStore** (核心) | duckdb.rs | 20 | 0 | ✅ 较好 |
| **ParserEngine** (解析器) | lib.rs | 17 | 0 | ✅ 较好 |
| **Adapter 规则引擎** | learn.rs, rule.rs, route.rs, sangfor.rs, generic.rs | 22 | 0 | ✅ 较好 |
| **API 路由** | handlers.rs | 19 | 3 (tokio) | ✅ 较好 |
| **Spool 持久队列** | segment.rs, replay.rs | 7 | 0 | ✅ 较好 |
| **Frozen 存储** | frozen.rs | 3 | 0 | ⚠️ 基本 |
| **DualDb 双库** | dual_db.rs | 4 | 0 | ⚠️ 基本 |
| **Governor 治理** | governor.rs | 1 | 0 | ❌ 不足 |
| **Hybrid 混合存储** | hybrid.rs | 0 | 0 | ❌ 缺乏 |
| **ClickHouse 存储** | clickhouse.rs | 0 | 1 (ignored) | ❌ 缺乏 |
| **Lifecycle 生命周期** | lifecycle.rs | 1 | 0 | ❌ 不足 |
| **Archive 归档** | archive.rs | 2 | 0 | ⚠️ 基本 |

---

## 二、新增测试：HybridStorage（高优先级）

**目标**: `crates/fwlog-storage/src/hybrid.rs` — 当前测试覆盖率为 0

### 2.1 单元测试

| # | 测试名称 | 描述 | 关键断言 |
|---|---------|------|---------|
| 1 | `insert_batch_writes_to_local_duckdb` | 向 HybridStorage 插入一批事件，验证写入 DuckDB | `local_count > 0` |
| 2 | `insert_batch_spawns_async_remote_write` | ClickHouse 启用时，验证异步远程写入被触发 | `remote.insert_batch` 被调用 |
| 3 | `insert_batch_backpressure_drops_remote` | 信号量耗尽时验证远程写入被跳过，本地写入正常 | 本地写入成功，远程被 drop |
| 4 | `insert_batch_clickhouse_disabled_no_remote` | ClickHouse 禁用时，验证不触发远程写入 | 仅本地写入 |
| 5 | `query_within_hot_window_uses_local` | date_from 在 hot_data_hours 内 → 路由到 DuckDB | 返回本地数据 |
| 6 | `query_outside_hot_window_uses_remote` | date_from 超出 hot_data_hours → 路由到 ClickHouse | 返回远程数据 |
| 7 | `query_remote_failure_falls_back_to_local` | ClickHouse 不可用 → 降级到 DuckDB 查询 | 不报错，返回本地数据 |
| 8 | `stats_reports_both_counts` | 验证 `HybridStats.local_count` 和 `remote_count` | 两个都为正数或无 |
| 9 | `health_check_reports_degraded_when_remote_down` | ClickHouse 不可用 → `remote_ok: false` | 健康状态显示降级 |
| 10 | `events_table_schema_matches_duckdb_columns` | 验证 ClickHouse events 表字段与 DuckDB 兼容 | 类型映射正确 |

### 2.2 实现建议

创建 `crates/fwlog-storage/src/hybrid.rs` 中的 `#[cfg(test)] mod tests`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use fwlog_domain::CanonicalEvent;

    fn mock_event(id: &str) -> CanonicalEvent { /* ... */ }

    fn create_hybrid_storage(clickhouse_enabled: bool) -> (HybridStorage, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.duckdb");
        let local = DuckDbStore::open(&path).unwrap();
        let config = HybridConfig {
            clickhouse_enabled,
            clickhouse_url: "http://localhost:8123".into(),
            clickhouse_database: "oxidelog_test".into(),
            hot_data_hours: 1,
            max_concurrent_writes: 16,
        };
        let runtime = tokio::runtime::Handle::current();
        (HybridStorage::new(path.clone(), local, config, runtime).unwrap(), path)
    }

    #[test]
    fn insert_batch_writes_to_local_duckdb() {
        let (storage, path) = create_hybrid_storage(false);
        let events = vec![mock_event("e1"), mock_event("e2")];
        storage.insert_batch(&events).unwrap();
        // Verify in DuckDB
        let store = DuckDbStore::open_read_only(&path).unwrap();
        let stats = store.event_stats().unwrap();
        assert_eq!(stats.total, 2);
    }

    #[tokio::test]
    async fn query_outside_hot_window_uses_remote() {
        // Requires running ClickHouse; use #[ignore] guard
        let (storage, _path) = create_hybrid_storage(true);
        let query = EventQuery {
            date_from: Some((Utc::now() - Duration::hours(48)).to_rfc3339()),
            ..Default::default()
        };
        let result = storage.query_events_with_query(&query, 10).await;
        assert!(result.is_ok());
    }
}
```

---

## 三、新增测试：Governor 治理循环（高优先级）

**目标**: `crates/fwlog-storage/src/governor.rs` — 当前 1 个测试

### 3.1 单元测试

| # | 测试名称 | 描述 | 关键断言 |
|---|---------|------|---------|
| 1 | `run_governance_cycle_archive_only` | 仅启用归档，验证归档阶段完成 | `report.archive_completed == true` |
| 2 | `run_governance_cycle_lifecycle_only` | 仅启用生命周期，验证压缩阶段完成 | `report.lifecycle_completed == true` |
| 3 | `run_governance_cycle_both` | 同时启用归档+生命周期，验证执行顺序 | 归档先于生命周期 |
| 4 | `run_governance_cycle_neither` | 两者都禁用 → 不执行任何操作 | 两个 completed 均为 false |
| 5 | `archive_before_lifecycle_ordering` | 验证归档在生命周期之前运行（关键！） | 归档报告先于生命周期 |
| 6 | `error_in_archive_does_not_block_lifecycle` | 归档失败后生命周期仍可执行 | lifecycle 正常完成 |

### 3.2 实现建议

```rust
#[test]
fn run_governance_cycle_both() {
    let dir = tempfile::tempdir().unwrap();
    let duckdb_path = dir.path().join("test.duckdb");
    let parquet_dir = dir.path().join("parquet");
    let frozen_dir = dir.path().join("frozen");
    
    // Seed data
    let store = DuckDbStore::open(&duckdb_path).unwrap();
    store.insert_batch(&test_events(100)).unwrap();
    
    let config = GovernorConfig {
        archive: ArchiveConfig { 
            enabled: true, interval_seconds: 3600, batch_limit: 100,
            parquet_retention_days: 90, frozen_retention_days: 180,
        },
        lifecycle: LifecycleConfig {
            enabled: true, hot_limit: 50, interval_seconds: 3600, drop_parsed_raw: true,
        },
    };
    
    let report = run_governance_cycle(
        &duckdb_path, &parquet_dir, &frozen_dir, &config, true, true
    ).unwrap();
    
    assert!(report.archive_completed);
    assert!(report.lifecycle_completed);
    assert!(report.archive_events <= 100);
}
```

---

## 四、新增测试：ClickHouse 存储（高优先级）

**目标**: `crates/fwlog-storage/src/clickhouse.rs` — 当前 1 个 ignored 测试

### 4.1 集成测试（使用 Docker ClickHouse）

在目标服务器上启动 ClickHouse 后运行（`docker compose -f docker-compose.clickhouse.yml up -d`）：

| # | 测试名称 | 标记 | 描述 |
|---|---------|------|------|
| 1 | `clickhouse_insert_and_query_roundtrip` | `#[ignore]` | 插入一批事件，查询返回，验证字段完整 |
| 2 | `clickhouse_query_complex_filters` | `#[ignore]` | 测试所有 EventQuery 字段的 WHERE 子句 |
| 3 | `clickhouse_query_by_day` | `#[ignore]` | 按天筛选历史数据 |
| 4 | `clickhouse_query_date_range` | `#[ignore]` | date_from + date_to 范围查询 |
| 5 | `clickhouse_escape_sql_prevents_injection` | `#[ignore]` | 验证 `escape_sql` 阻止单引号注入 |
| 6 | `clickhouse_count_and_database_size` | `#[ignore]` | `count_events()` 和 `database_size()` 返回合理值 |
| 7 | `clickhouse_ping_and_table_ready` | `#[ignore]` | 健康检查和表存在性验证 |
| 8 | `canonical_event_to_ch_roundtrip_preserves_all_fields` | `#[ignore]` | 验证 CanonicalEvent → ClickHouseEvent → CanonicalEvent 字段完整 |

### 4.2 Schema 兼容性验证

需要验证 DuckDB → ClickHouse 字段映射：

| DuckDB 字段 | ClickHouse 类型 | 兼容性关注点 |
|-------------|----------------|-------------|
| `ingest_time TEXT` | `DateTime64(3, 'UTC')` | 时间戳精度、时区 |
| `src_ip TEXT / NULL` | `String / ""` | 空字符串 vs NULL 转换 |
| `parse_status TEXT` | `LowCardinality(String)` | 枚举值映射 |
| `raw TEXT` | `String CODEC(ZSTD(3))` | 压缩效果 |
| `src_port INTEGER / NULL` | `UInt16 / 0` | 0 vs NULL 语义 |

---

## 五、新增测试：DualDb 双库读写分离（中优先级）

**目标**: `crates/fwlog-storage/src/dual_db.rs` — 当前 4 个测试

### 5.1 补充测试

| # | 测试名称 | 描述 |
|---|---------|------|
| 1 | `dual_db_concurrent_write_and_query` | 写入 DB 后，同步到查询 DB，验证查询 DB 读取到新数据 |
| 2 | `dual_db_sync_after_rotation` | 切换写入目标后同步仍正常工作 |
| 3 | `dual_db_sync_empty_state` | 没有新数据时同步不报错 |
| 4 | `dual_db_metrics_updated_on_sync` | 每次同步后 metrics 计数器递增 |

---

## 六、服务端冒烟测试扩展（中优先级）

**目标**: `scripts/smoke-production.sh` — 当前覆盖基本路径

### 6.1 ClickHouse 存储验证步骤

新增 `--clickhouse` 选项，启用后增加：

```bash
# 步骤: ClickHouse 健康检查
step "GET ${base_url}/api/storage/health"
# 验证 remote_ok = true (如果 ClickHouse 已配置)
# 验证 remote_schema_ok = true

# 步骤: ClickHouse 存储统计
step "GET ${base_url}/api/storage/stats"
# 验证 remote_count 存在且大于 0

# 步骤: 冷数据查询路由
step "GET ${base_url}/api/events?date_from=2days_ago&limit=10"
# 如果 ClickHouse 有历史数据，验证路由正常工作
# 如果 ClickHouse 无历史数据，验证降级到 DuckDB

# 步骤: 只启用 DuckDB 验证（--no-clickhouse）
# 验证 501 Not Implemented 降级
storage_health_status=$(get_json_status "/api/storage/health" "$output_dir/storage-health.json")
if [ "$storage_health_status" = "501" ]; then
  step "hybrid storage endpoints not enabled; running in DuckDB-only mode"
fi
```

### 6.2 混合模式场景

| 模式 | CLI 参数 | 预期行为 |
|------|---------|---------|
| 仅 DuckDB (默认) | 不加 `--clickhouse` | `storage/health` 返回 501 |
| DuckDB + ClickHouse | `--clickhouse` | `storage/health` 返回 `local_ok: true, remote_ok: true` |
| ClickHouse 不可用 | `--clickhouse --clickhouse-unavailable` | `storage/health` 返回 `local_ok: true, remote_ok: false` |

---

## 七、前端 E2E 测试（低优先级）

**目标**: `ant-design-pro-6.0.1/` 前端

### 7.1 推荐测试点

| # | 测试场景 | 操作 |
|---|---------|------|
| 1 | 日志搜索 | 打开 OxideLog 搜索面板，输入过滤条件，验证结果展示 |
| 2 | 仪表盘概览 | 加载 Overview 页面，验证图表渲染 |
| 3 | 源管理 | 查看 Source Governance 页面，验证设备列表 |
| 4 | 解析器管理 | 查看 Parser 页面，验证规则展示 |
| 5 | 资产配置 | 查看 Assets 页面，验证 IP 区域和设备配置 |

---

## 八、性能测试（中优先级）

### 8.1 基准测试场景

| # | 测试名称 | 测量指标 | 预期基准 |
|---|---------|---------|---------|
| 1 | 10K 批量插入 | 耗时、吞吐量 (events/sec) | `insert_batch(10000)` < 500ms |
| 2 | 100K 批量插入 | 耗时、内存使用 | `insert_batch(100000)` < 5s |
| 3 | 1M 行查询 | 全表扫描 + 过滤 | 带索引查询 < 100ms |
| 4 | 并发读写 | 读不会阻塞写 | 写时查询延迟 < 200ms |
| 5 | ClickHouse 批量写入 | 10K → ClickHouse 耗时 | `insert_batch(10000)` < 1s |

### 8.2 压测建议

使用 `wrk` 或 `hey` 对 API 端点进行基本负载测试：

```bash
# 查询 API 压测
hey -n 1000 -c 10 "http://server:18080/api/events?limit=50"

# 并发写入压测 (通过 TCP syslog)
python3 -c "
import socket, time
s = socket.socket()
s.connect(('server', 1514))
for i in range(10000):
    s.send(f'<134>1 {time.strftime(\"%Y-%m-%dT%H:%M:%S\")} test-host oxidelog - - - allow src=10.0.{i//256}.{i%256} dst=10.0.0.1 action=allow\n'.encode())
s.close()
"
```

---

## 九、测试优先级与执行顺序

### 第一阶段：关键路径（目标：2-3 天）

```
1. HybridStorage 单元测试（hybrid.rs）       ← 最高优先级
2. Governor 治理循环测试（governor.rs）       ← 最高优先级
3. ClickHouse 存储集成测试（clickhouse.rs）    ← 最高优先级
4. 更新 smoke-production.sh 增加 ClickHouse 验证 ← 高优先级
```

### 第二阶段：增强覆盖（目标：1-2 周）

```
5. DualDb 测试补充（dual_db.rs）
6. 服务端部署冒烟测试
7. Schema 兼容性验证
```

### 第三阶段：持续改进（长期）

```
8. 前端 E2E 测试
9. 性能基准测试
10. 压力测试与稳定性验证
```

---

## 十、运行测试指南

### 在目标 Linux 服务器上

```bash
# 1. 启动 ClickHouse（如果测试需要）
docker compose -f docker-compose.clickhouse.yml up -d
# 初始化事件表
docker exec -i $(docker ps -q -f name=clickhouse) clickhouse-client --multiquery < scripts/clickhouse-init.sql

# 2. 运行存储层单元测试（忽略 ClickHouse 集成测试）
cargo test -p fwlog-storage -- --skip clickhouse

# 3. 运行全量测试（包含 ClickHouse 集成测试）
cargo test -p fwlog-storage -- --include-ignored

# 4. 运行 API 集成测试
cargo test -p fwlog-api

# 5. 运行 fwlogd 单元测试
cargo test -p fwlogd

# 6. 运行生产冒烟测试
scripts/smoke-production.sh \
  --api-host <server_ip> \
  --clickhouse \
  --clickhouse-host <clickhouse_host>
```

### 在本地（仅检查和文档验证）

```bash
# 仅允许的本地操作
# - 检查测试代码语法风格
# - 验证测试计划文档
# - 检查测试覆盖率报告
# 不允许:
# - cargo test / cargo build / cargo check
```

---

## 十一、测试验收标准

| 标准 | 要求 |
|------|------|
| **新增测试** | HybridStorage ≥ 10 个，Governor ≥ 6 个，ClickHouse ≥ 8 个 |
| **冒烟测试** | 覆盖 `/api/storage/health` 和 `/api/storage/stats` |
| **ClickHouse 兼容** | 所有 DuckDB 字段通过 roundtrip 验证 |
| **降级路径验证** | ClickHouse 不可用时系统降级到 DuckDB 且不报错 |
| **治理循环** | 归档和生命周期串行执行，失败互不影响 |
| **性能基准** | 10K 插入 < 500ms，1M 索引查询 < 100ms |

---

## 附录：现有测试清单

<details>
<summary>点击展开完整现有测试清单（143 个）</summary>

- **duckdb.rs** (20): initializes_parser_adaptive_tables, reads_source_aliases, checkpoints_complete_adaptive_state, prune_parsed_raw, compact_hot, initialize_inserts_queries_exports, archives_recent_events, archives_selected_events, reports_event_stats, maintains_minute_metrics, reports_source_metrics, query_events_filters, include_failed_false, archive_index_filters_by_day, query_events_filters_by_device_id, backfills_device_ids, stores_ip_region_cache, migrates_existing_database, replace_all_events_preserves_ids, replace_all_events_rejects_empty, replace_all_events_rejects_changed, compacts_to_new_database, compacts_with_hot_limit, compacts_with_limit, archives_slim_parquet
- **lib.rs (adapter)** (17): 4 sangfor tests, 6 generic tests, 7 engine tests
- **learn.rs** (11): active_rule_fills_empty, active_rule_does_not_overwrite, shadow_rule_activates, auto_activate_false, active_rule_disabled, active_rule_records_failed, failed_rule_conflicts, observe_parse_result_learns, candidate_from_pair_no_action, candidate_from_pair_no_dst_ip, candidate_from_pair_no_level
- **handlers.rs** (22): 19 HTTP API tests + 3 utility tests
- **其他** (16): spool (7), frozen (3), archive (2), lifecycle (1), ingress tcp+udp (2), domains (1)

</details>