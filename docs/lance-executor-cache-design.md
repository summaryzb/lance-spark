# Lance Executor Disk Cache 设计文档

**状态**：✅ 已实施（V3 per-column + consistent hash affinity）
**日期**：2026-05-15
**基于数据**：
- 103 TPC-DS SF-100 实测（local-cluster）：
  - baseline (cache OFF): 4240s
  - 8×1-core + consistent hash affinity: 3074s (-27.5%)
  - 2×4-core + consistent hash affinity: 3024s (-28.7%)
  - hit rate: 95.1%

---

## 1 · 核心决策

| 维度 | 决策 |
|---|---|
| Cache 层 | lance-spark（不动 lance-core）|
| 拦截点 | `LanceFragmentScanner.getArrowReader()` |
| Cache key | `(datasetUri, pinnedVersion, fragmentId, batchSize, authOptsDigest)` — **不含列投影、不含 filter** |
| Cache value | 每列独立的 Arrow IPC stream 文件 |
| 目录结构 | `{cacheDir}/executor-{id}/{fingerprint}/{colName}.arrow` |
| 存储位置 | env `LANCE_EXEC_CACHE_DIR`（默认 `spark.local.dir/lance-cache`）|
| 每 executor 容量 | 独立预算，env `LANCE_EXEC_CACHE_DISK_LIMIT_GB`（默认 30 GB）|
| 淘汰策略 | LRU（按最近访问时间），淘汰单位 = 整个 fragment 目录 |
| 并发 | per-fingerprint `ReentrantLock`，eviction 在 lock 外执行 |
| 调度亲和性 | **一致性 hash**（fragment fingerprint → executor），`preferredLocations` 实现 PROCESS_LOCAL |
| Filter pushdown | cache ON 时 **禁用**（`pushFilters()` early return）。Spark Filter 算子负责过滤 |
| 部分命中 | ✅ query [a,b,c] 后 query [b,c,d] → b,c 从 cache 读，只 decode d |
| 失效 | version 变化自然隔离（pinnedVersion 在 key 里）；无 TTL |
| Crash 恢复 | 启动时 `rebuildIndex()` 扫子目录重建 LRU；删除 `.tmp` 文件 |
| Metrics | Spark Metrics Source（Dropwizard），暴露 hits/misses/partialHits/hitRate/evictions/entries/diskBytes |

---

## 2 · 架构概图

```
Driver JVM                                Executor JVM
──────────                                ────────────
LanceScanBuilder.build()                  LanceFragmentScanner.getArrowReader()
  ↓ 注册 LanceSoftAffinityListener         ↓
  ↓                                       ┌─────────────────────────────────────────┐
LanceScan.planInputPartitions()           │ LanceExecutorCache.getOrLoadColumns(    │
  ↓ 查 LanceSoftAffinityManager            │   key, requestedColumns, allocator,     │
  ↓ hash(fingerprint) → executor          │   loader)                               │
  ↓ 设 preferredLocations                  │                                         │
  ↓                                       │ 全 hit  → 打开 N 个 {col}.arrow         │
LanceInputPartition                       │        → ColumnAssemblingArrowReader     │
  .preferredLocations()                   │                                         │
  = ["executor_{host}_{id}"]              │ 部分 hit → hit 列从 cache 读             │
                                          │          → miss 列调 loader + 写 cache   │
DAGScheduler                              │                                         │
  → PROCESS_LOCAL 调度                     │ 全 miss → loader decode + 逐列写入       │
  → task 落在有 cache 的 executor          └─────────────────────────────────────────┘
```

---

## 3 · 一致性 Hash 调度亲和性

### 原理

- 每个 executor 在 hash ring 上有 100 个虚拟节点
- fragment fingerprint 通过 SHA-256 hash 映射到 ring 上的 slot
- 从 slot 顺时针找到第一个 executor → 设为 `preferredLocations`
- 同一 fragment 总映射到同一 executor（executor 集合不变时）
- executor 增减时一致性 hash 最小化 key 迁移

### 组件

| 组件 | 位置 | 职责 |
|---|---|---|
| `LanceConsistentHash<T>` | internal | 线程安全的一致性 hash ring |
| `LanceSoftAffinityManager` | internal | Driver 单例，管理 ring + executor 列表 |
| `LanceSoftAffinityListener` | Scala, spark 包 | SparkListener 监听 executor 增减 |

### 行为

- **首次 query**：hash 确定性，fragment 立即映射到固定 executor → miss 后 cache 写入正确位置
- **后续 query**：同一 fragment 映射到同一 executor → cache hit
- **executor 增减**：ring rebalance，只有受影响的 fragment 重新映射
- **executor 失效**：SparkListener 从 ring 移除，后续 query 映射到新 executor

### 与 Gluten SoftAffinity 的关系

参考 Gluten 的 `ConsistentHash.java` + `SoftAffinityManager.scala` 设计，独立实现。
区别：Gluten 用 MurmurHash3，我们用 SHA-256（JVM 保证可用）；Gluten 支持 FORCE 模式，我们只用 SOFT。

---

## 4 · 关键文件

| 文件 | 职责 |
|---|---|
| `LanceExecutorCache.java` | per-column 目录结构 + getOrLoadColumns + LRU eviction + metrics |
| `LanceExecutorCacheKey.java` | 不可变 cache key + SHA-256 fingerprint |
| `ColumnAssemblingArrowReader.java` | 拼装 N 个单列 ArrowReader 为多列 VectorSchemaRoot |
| `LanceConsistentHash.java` | 一致性 hash ring（ThreadLocal SHA-256, ReadWriteLock） |
| `LanceSoftAffinityManager.java` | Driver 单例，executor ring + preferredLocations 计算 |
| `LanceSoftAffinityListener.scala` | SparkListener 监听 executor 生命周期 |
| `LanceExecutorCacheMetricsSource.java` | Spark Metrics Source（Dropwizard gauges） |
| `LanceFragmentScanner.java` | 集成点：getArrowReader() 调用 cache API |
| `LanceScanBuilder.java` | pushFilters() cache guard + listener 注册 |
| `LanceScan.java` | planInputPartitions() 查 affinity manager |
| `LanceInputPartition.java` | preferredLocations 字段 |

---

## 5 · 配置

| Env 变量 | 默认值 | 说明 |
|---|---|---|
| `LANCE_EXEC_CACHE_ENABLED` | `false` | 总开关（opt-in）|
| `LANCE_EXEC_CACHE_DIR` | `spark.local.dir/lance-cache` | 磁盘根目录（每 executor 自动加 `executor-{id}` 子目录）|
| `LANCE_EXEC_CACHE_DISK_LIMIT_GB` | `30` | 每 executor 磁盘上限 |

local-cluster / standalone 模式下需同时设 `spark.executorEnv.LANCE_EXEC_CACHE_*`。

---

## 6 · 性能数据

### 103 TPC-DS SF-100

| 配置 | wall (s) | hit rate | cache 大小 | vs baseline |
|---|---|---|---|---|
| baseline (8×1, cache OFF) | 4240 | — | 0 | — |
| 8×1 + affinity | 3074 | 95.1% | 207 GB | -27.5% |
| 2×4 + affinity | 3024 | ~95% | 156 GB | -28.7% |

### 设计演进

| 版本 | 描述 | hit rate | 结论 |
|---|---|---|---|
| V1 | post-filter cache（filter 进 key） | 0.1% | killed |
| V2 | pre-filter, per-projection | ~42% (local) | 有正确性 bug |
| V3 无 affinity | pre-filter, per-column, 无调度亲和 | 3.9% (8-exec) | 调度稀释 |
| **V3 + consistent hash** | pre-filter, per-column, 一致性 hash | **95.1%** | **当前** |

---

## 7 · 已知限制

1. **Filter pushdown 禁用**：cache ON 时 Lance 不做行级过滤，Spark Filter 算子兜底。对高选择率 query 有额外 I/O 开销。
2. **miss 路径不做列级 scan**：partial miss 时 loader 返回全部投影列，多余列丢弃。
3. **executor 黑名单**：当前不监听 `onExecutorExcluded`，被排除的 executor 仍在 ring 中。
4. **首次 query 全 miss**：一致性 hash 保证后续 query 命中，但第一个 query 的所有 fragment 都是 cold miss。
5. **虚拟节点数固定 100**：未做可配置化。

---

## 8 · 排除的方向

| 方向 | 原因 |
|---|---|
| RPC 汇报方案 | 复杂度高、依赖 Spark private API、首次运行无效 |
| RocksDB 存储 | 大 value 不适合 LSM；写放大 + compaction 抖动 |
| 内存 cache | 用户明确排除（内存不足） |
| 全列存 + 读时裁剪 | hit 路径读多余数据；miss 路径 decode 全列代价大 |
| 格式级改造 | 用户排除 |
| 动 lance-core | 全在 lance-spark 完成 |
