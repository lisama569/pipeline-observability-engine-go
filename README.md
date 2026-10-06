# Pipeline Observability Engine（进程内，Go 标准库）

在单进程内模拟多条数据流水线的运行遥测接入、聚合、时效判定与告警，并用本地
JSON 快照持久化。**无外部服务、无网络、无真实凭据**，结论在并发、乱序、
延迟、时钟回拨与重启场景下保持一致且可解释。

## 能力一览

- 多流水线 / 多算子事件接入；并发安全。
- 幂等去重（同一 `Event.ID` 只计一次，并发投递也只生效一次）。
- 事件时间窗口聚合；容忍窗口内的迟到事件并入旧窗口，超窗事件**明确标记
  `expired` 并留痕**（不静默丢弃）。
- 数据滞后（`data_stale`）、任务卡住无进展（`task_stuck`，只靠真实时钟推进、
  无任何新事件也能判出）、任务超时（`task_timeout`，按事件时间水位判定）、
  执行失败（`task_failed`）四类原因可区分；阈值可配、边界口径唯一（均为 `>=`）。
- 告警触发 / 去重抑制 / 静默 / 恢复：同一根因一轮只通知一条，恢复后再次恶化
  重新触发；**重启后未恢复告警与静默状态延续，不刷屏、不丢失**。
- **按范围静默**：全局 / 整条流水线 / 单个算子（一整类问题）/ 单原因 /
  单次任务失败，含“已在报”和“静默期内新冒出”的问题；只压对外通知，
  内部状态机照常推进并持久化；静默到期或手动解除后，问题仍在即重新报警。
- **只读运行健康汇总** `HealthSummary()`：按流水线、算子聚合是否卡住/滞后、
  当前报警数与原因集合，与逐条判定和 `ActiveAlerts()` 严格同源对账。
- 处理时钟回拨时有效时间与水位保持单调，结论稳定可复现。

## 快速开始

```bash
go test -race -count=5 ./...      # 全套单测（日志含关键事件与判定依据）
go run ./cmd/demo                 # 首次运行，生成本地快照 demo-state.json
go run ./cmd/demo                 # 再次运行即“进程重启恢复”
```

最小用法：

```go
cfg := pipelineobs.DefaultConfig()
clk := pipelineobs.NewMockClock(time.Now().UTC())      // 生产用 pipelineobs.SystemClock{}
eng, _ := pipelineobs.Open(ctx, cfg, clk, pipelineobs.NewFileStore("state.json"))
defer eng.Close(ctx)

res, _ := eng.Ingest(ctx, pipelineobs.Event{
    ID: "evt-1", Pipeline: "p1", Operator: "source",
    Kind: pipelineobs.KindRecord, OccurredAt: dataTime, Bytes: 42,
})
// res.Verdict: Accepted / Duplicate / Expired / Reason / Detail
// res.Alerts / res.Recovered: 本次新产生与恢复的告警
```

## 文档

- 语义与规则的完整说明见 [`docs/DESIGN.md`](docs/DESIGN.md)：事件模型与时间
  语义、窗口与阈值口径、告警去重/静默/恢复状态机、持久化协议与全部配置项。

## 代码结构

| 文件 | 职责 |
|---|---|
| `clock.go` | 时钟抽象（系统时钟 / 可控 mock，支持回拨） |
| `model.go` | 事件、判定结论、原因码 |
| `state.go` | 快照结构、窗口/键规则、加载规范化 |
| `storage.go` | 持久化接口与原子 JSON 文件存储 |
| `engine.go` | 接入主链路：去重 → 迟到/过期 → 聚合 → 评估 → 刷盘 |
| `alert.go` | 告警状态机：触发、抑制、静默闸门、卡住/超时/失败评估、恢复 |
| `silence.go` | 按范围静默规则管理（全局/流水线/算子/原因/单次任务） |
| `health.go` | `HealthSummary` 只读健康汇总（与活动告警同源） |
| `query.go` | `ObserveTime` 与只读视图（窗口、活动告警） |
| `cmd/demo` | 本地端到端演示（卡住、范围静默、健康汇总、重启恢复） |
