# Lance Executor Disk Cache 实施进度

**状态**：✅ **V3 per-column + consistent hash affinity 完成**
**设计文档**：[`lance-executor-cache-design.md`](./lance-executor-cache-design.md)

---

## 最终结果（2026-05-15）

### 103 TPC-DS SF-100 benchmark

| 配置 | wall (s) | hit rate | cache 大小 | vs baseline |
|---|---|---|---|---|
| baseline (8×1, cache OFF) | 4240 | — | 0 | — |
| 8×1 + consistent hash affinity | 3074 | 95.1% | 207 GB | -27.5% |
| 2×4 + consistent hash affinity | 3024 | ~95% | 156 GB | -28.7% |

103/103 queries PASS，0 failures。

### 设计演进

| 版本 | 描述 | hit rate | 结论 |
|---|---|---|---|
| V1 | post-filter cache（filter 进 key） | 0.1% | killed |
| V2 | pre-filter, per-projection | ~42% (local) | 有正确性 bug |
| V3 无 affinity | pre-filter, per-column, 无调度亲和 | 3.9% (8-exec) | 调度稀释 |
| **V3 + consistent hash** | pre-filter, per-column, 一致性 hash | **95.1%** | **最终方案** |

---

## 分支

| 分支 | 说明 |
|---|---|
| `feat/option-b-driver-cache` | 基础 per-column cache（无 affinity） |
| `feat/cache-aware-scheduling` | RPC 汇报方案（备选，已弃用） |
| `feat/consistent-hash-affinity` | 一致性 hash affinity（含 metrics.json） |
| **`feat/consistent-hash-affinity-clean`** | **最终版本**（去掉 metrics.json 落盘） |
