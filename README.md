# Pipeline Observability Engine（进程内，Go 标准库）

在单进程内模拟多条数据流水线的运行遥测接入、聚合、时效判定与告警，并用本地
JSON 快照持久化。**无外部服务、无网络、无真实凭据**，结论在并发、乱序、
延迟、时钟回拨与重启场景下保持一致且可解释。

## 能力一览

- 多流水线 / 多算子事件接入；并发安全。
- 幂等去重（同一 `Event.ID` 只计一次，并发投递也只生效一次）。
- 事件时间窗口聚合；容忍窗口内的迟到事件并入旧窗口，超窗事件**明确标记
  `expired` 并留痕**（不静默丢弃）。
- 数据滞后（`data_stale`）、任务超时（`task_timeout`，按事件时间水位判定）、
  任务卡死（`task_stuck`，运行中但算子完全无进展，纯时钟推进即可判出）、
  执行失败（`task_failed`）四类原因可区分；阈值可配、边界口径唯一（均为 `>=`）。
- 告警触发 / 去重抑制 / 静默 / 恢复：同一根因一轮只通知一条，恢复后再次恶化
  重新触发；**重启后未恢复告警与静默状态延续，不刷屏、不丢失**。
- 按范围静默（`SilenceScoped`）：一次静默整条流水线或某算子名下全部问题
  （已报的 + 静默期内新冒出来的），只压对外通知、状态机照常推进；到期或
  手动解除后问题仍在即重新报警。
- 运行健康汇总（`Health()`）：只读、按流水线/算子聚合的卡死/滞后/活动告警
  一览，与逐条判定及活动告警严格一致。
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
| `alert.go` | 告警状态机：触发、抑制、静默匹配、恢复；卡死判定 |
| `silence.go` | 静默规则管理（逐根因 + 按范围） |
| `query.go` | `ObserveTime` 与只读视图（窗口、活动告警、健康汇总） |
| `cmd/demo` | 本地端到端演示 |
