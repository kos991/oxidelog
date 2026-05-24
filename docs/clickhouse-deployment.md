# ClickHouse 集成部署指南

## 快速启动

### 1. 启动 ClickHouse

```bash
# 使用 Docker Compose 启动
docker-compose -f docker-compose.clickhouse.yml up -d clickhouse

# 等待 ClickHouse 就绪
docker-compose -f docker-compose.clickhouse.yml logs -f clickhouse
# 看到 "Ready for connections" 即可
```

### 2. 验证 ClickHouse

```bash
# 测试连接
curl http://localhost:8123/ping

# 查看数据库
docker exec -it oxidelog-clickhouse clickhouse-client --query "SHOW DATABASES"

# 查看表结构
docker exec -it oxidelog-clickhouse clickhouse-client --query "SHOW TABLES FROM oxidelog"
```

### 3. 启用混合存储

编辑 `config/server.toml`:

```toml
[storage]
mode = "hybrid"  # 从 "local" 改为 "hybrid"

[storage.clickhouse]
enabled = true   # 从 false 改为 true
url = "http://localhost:8123"
database = "oxidelog"
hot_data_hours = 1
```

### 4. 重启 fwlogd

```bash
# 如果使用 Docker
docker-compose -f docker-compose.clickhouse.yml restart fwlogd

# 如果本地运行
cargo build --release
./target/release/fwlogd
```

## 数据迁移（可选）

如果你有历史数据需要导入到 ClickHouse：

```bash
# 1. 从 DuckDB 导出为 Parquet
# TODO: 添加导出工具

# 2. 导入到 ClickHouse
clickhouse-client --query "INSERT INTO oxidelog.events FORMAT Parquet" < data.parquet
```

## 监控

### 查看存储统计

```bash
# ClickHouse 数据量
docker exec -it oxidelog-clickhouse clickhouse-client --query "
SELECT 
    count() as total_events,
    formatReadableSize(sum(bytes_on_disk)) as disk_size,
    formatReadableSize(sum(data_compressed_bytes)) as compressed_size,
    round(sum(data_compressed_bytes) / sum(data_uncompressed_bytes) * 100, 2) as compression_ratio
FROM system.parts 
WHERE database = 'oxidelog' AND active
"
```

### 查看物化视图

```bash
# 分钟级指标
docker exec -it oxidelog-clickhouse clickhouse-client --query "
SELECT bucket_minute, sum(total_count) as events
FROM oxidelog.mv_minute_metrics
GROUP BY bucket_minute
ORDER BY bucket_minute DESC
LIMIT 10
"
```

## 压缩效果验证

```bash
# 查看压缩率
docker exec -it oxidelog-clickhouse clickhouse-client --query "
SELECT 
    table,
    formatReadableSize(sum(data_uncompressed_bytes)) as uncompressed,
    formatReadableSize(sum(data_compressed_bytes)) as compressed,
    round(sum(data_compressed_bytes) / sum(data_uncompressed_bytes) * 100, 2) as ratio_percent
FROM system.parts
WHERE database = 'oxidelog' AND active
GROUP BY table
"
```

预期压缩率：**5-10%**（即 10-20x 压缩）

## 故障排查

### ClickHouse 无法连接

```bash
# 检查容器状态
docker ps | grep clickhouse

# 查看日志
docker logs oxidelog-clickhouse

# 测试端口
curl http://localhost:8123/ping
```

### 数据未同步

```bash
# 检查 fwlogd 日志
docker logs oxidelog-fwlogd | grep clickhouse

# 手动插入测试
docker exec -it oxidelog-clickhouse clickhouse-client --query "
INSERT INTO oxidelog.events VALUES (
    'test-id',
    now(),
    '192.168.1.1',
    'device-1',
    now(),
    'test',
    'test',
    '10.0.0.1',
    80,
    '10.0.0.2',
    443,
    'tcp',
    'allow',
    'info',
    'test raw log',
    'parsed',
    ''
)
"
```

## 性能调优

### 调整 TTL（数据保留期）

```sql
-- 修改主表 TTL 为 180 天
ALTER TABLE oxidelog.events MODIFY TTL toDate(ingest_time) + INTERVAL 180 DAY DELETE;

-- 修改物化视图 TTL 为 60 天
ALTER TABLE oxidelog.mv_minute_metrics MODIFY TTL toDate(bucket_minute) + INTERVAL 60 DAY DELETE;
```

### 调整分区策略

```sql
-- 查看分区大小
SELECT 
    partition,
    count() as rows,
    formatReadableSize(sum(bytes_on_disk)) as size
FROM system.parts
WHERE database = 'oxidelog' AND table = 'events' AND active
GROUP BY partition
ORDER BY partition DESC
LIMIT 10;
```

## 回滚到纯 DuckDB

如果需要禁用 ClickHouse：

```toml
[storage]
mode = "local"

[storage.clickhouse]
enabled = false
```

重启后系统将只使用 DuckDB。ClickHouse 数据不会丢失，可随时重新启用。
