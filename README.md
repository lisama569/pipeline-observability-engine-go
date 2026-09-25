# Pipeline Observability Engine (Go, in-process)

一个**完全本地运行**的流水线可观测性组件：在单进程内并发接入多条数据流水线
（pipeline）/ 算子（operator）的运行事件，进行去重聚合、乱序/迟到归并、
新鲜度与任务时效判定、告警触发/抑制/静默/恢复，并把全部状态原子持久化到本地
JSON 文件，使**并发、乱序与进程重启**场景下的统计与告警结论一致、可复现、可解释。

* 仅依赖 Go 标准库；无外部服务、无网络、无凭据。
* 时间由可注入的 `Clock` 提供，测试使用确定性假时钟（可模拟时钟回拨）。
* 所有公共 API 均并发安全；决策（去重+窗口+聚合+告警迁移）在同一把锁内原子完成。

## 快速开始

```bash
go test -race -count=1 ./...   # 全部单测（并发/乱序/迟到/重复/重启恢复）
go run ./cmd/demo              # 本地可复现的脚本化演示（自动清理快照）
```

演示输出中会逐条打印事件的接入判定（accepted / duplicate / EXPIRED）以及
告警（ALERT）与恢复（RECOVER）及其判定依据。

## 包结构

| 包 | 职责 |
|---|---|
| `internal/model` | 事件模型、聚合结构、告警模型、配置与边界口径 |
| `internal/clock` | 时间抽象：`RealClock` 与可回拨的 `FakeClock` |
| `internal/store` | 本地 JSON 快照：临时文件 + fsync + 原子 rename |
| `internal/alert` | 告警指纹、触发、抑制窗口、静默、恢复/再触发 |
| `internal/engine` | 并发接入、去重、水位与迟到/过期、聚合、条件评估、重启恢复 |

## 1. 事件模型与时间语义

### 事件

```go
type Event struct {
    ID         string    // 全局唯一投递 ID，幂等去重的依据
    PipelineID string
    OperatorID string
    RunID      string    // 任务生命周期事件必填
    Type       EventType // task_started/task_succeeded/task_failed/
                         // record_written/watermark
    EventTime  time.Time // 事件时间（业务时间）
    Records    int64     // record_written 的记录数
    Error      string    // task_failure 必填的失败原因
}
```

### 两条时间线

* **事件时间 EventTime**：事件声称发生的业务时间，是**聚合与乱序归并的唯一
  排序权威**。迟到、乱序、跨窗口都以它为准。
* **处理时间（ingest / processing time）**：引擎接入事件时由注入 `Clock` 读到
  的时间，仅用于数据新鲜度（data freshness）记账，以及持久化时间戳。

二者分离保证了：同一批事件无论以什么顺序、由几个 goroutine 投递，只要事件时间
相同，最终聚合与判定就相同。

### 单调处理时钟（时钟回拨保护）

引擎内部维护 `monoNow = max(历史见过的最大墙钟时间, 当前墙钟)`。即使墙钟发生
NTP 式回拨：

* 已流逝的时长不会"变负"，评估用的 elapsed 一律 `max(0, now-ref)`；
* 回拨本身**不会**把真实的数据滞后/超时判为恢复，也不会重复触发告警；
* 重启后 `monoNow` 从快照的 `SavedAt` 继续，单调性跨进程延续。

## 2. 接入结果：去重、迟到、过期（绝不静默丢弃）

每次 `Ingest` 都返回明确分类，互斥且可解释：

| 结果 | 含义 |
|---|---|
| `Accepted` | 计入聚合 |
| `Duplicate` | 同一 `ID` 的重复投递，**任何计数都不会二次变化**（计数器外） |
| `Expired` | 超出容忍窗口，**显式标记**并计入 `ExpiredEvents`，不静默丢弃 |
| 两者都不是 + `Reason` | 畸形事件（缺字段/类型非法/生命周期缺 RunID 等）被拒绝 |

### 去重（exactly-once accounting）

`seen[eventID] = EventTime`。第一次投递是唯一权威；重复投递只递增
`DuplicateEvents`。该集合随快照持久化，**重启后重复投递依然幂等**。
迟到事件在第一次到达时也会入集合，因此它的重投同样被识别为重复。

### 水位（watermark）与容忍窗口

算子可通过 `watermark` 事件声明水位（随事件时间单调递增）：

* 尚未收到任何水位时，**没有事件会过期**；
* 边界 = `watermark - LatenessTolerance`；
* `EventTime >= 边界`（**闭区间**，恰好在边界上不算过期）→ 接受并并入既有统计；
* `EventTime <  边界` → 标记为 `Expired`，给出含两个时间戳的理由字符串，
  其 ID 仍进入去重集合，过期计数可观测。

### 乱序归并（运行生命周期）

每个 `RunID` 的状态按事件时间归并：事件时间更大者胜；事件时间相同则按
**终态优先级 `failed > succeeded > started`** 唯一裁定（与到达顺序、map
遍历顺序无关）。迟到的成功/失败会纠正早先结论：例如先到 failure、后到
事件时间更晚的 success，最终判定成功，原 failure 告警随之恢复。

## 3. 新鲜度与任务时效判定（原因可区分）

评估是 `(配置, 统计, now)` 的**纯函数**，对每个已知算子恒定输出三类条件各一条
（active 或非 active），从而让恢复路径闭合：

| Reason | 触发条件 | 恢复条件 |
|---|---|---|
| `data_lag`（数据滞后） | 距上一个携带新鲜度的事件（`record_written`/`watermark`，按处理时间）已严格超过 `DataLagThreshold` | 收到新的 `record_written`/`watermark` |
| `task_timeout`（任务运行超时） | 最新一次运行仍为 `started`，且运行时长严格超过 `TaskTimeout` | 该运行到达终态（成功/失败） |
| `task_failure`（执行失败） | 最新一次运行终态为 `failed`（保留错误原因） | 后续运行成功/开始（新的最新运行非 failed） |

三种原因是**不同根因**（指纹不同），可同时存在，互不掩盖；data_lag 恢复不会
清掉 task_timeout。只有曾观测到数据/运行的算子才参与相应判定。

### 阈值边界口径（全局唯一）

* 阈值比较一律使用**严格大于**：`elapsed == threshold` 仍判为健康，
  `elapsed > threshold` 才触发；
* 迟到窗口使用闭区间（见上）；
* 静默区间为半开 `[StartsAt, EndsAt)`，`now == EndsAt` 时静默已失效。

## 4. 告警：触发、去重抑制、静默、恢复

告警指纹（根因键）：

```
Fingerprint = PipelineID | OperatorID | Reason
```

状态机（`internal/alert/manager.go`）：

1. **触发**：坏条件首次出现且未被静默 → 产生一条 active 告警（`FiredCount=1`）。
2. **抑制窗口内去重**：同一指纹持续为坏时，仅当
   `now - LastFiredAt > AlertSuppressWindow`（严格大于，边界仍抑制）才再通知
   一次；否则刷新但不产生新告警。**同一根因一次恶化只产生一条有效告警**。
3. **静默 silence**：`AddSilence(fingerprint, comment, start, end)`；`"*"` 为全
   局通配。静默期间该指纹不开新告警；静默结束后立刻可触发。
4. **恢复**：条件转好 → active 告警变为 resolved（带恢复依据文案）。
5. **恢复后再恶化**：立即作为新一轮 episode 重新触发（不等抑制窗口）。

### 重启后的告警与静默延续

快照保存全部 active/resolved 告警（含 `FirstFiredAt`/`LastFiredAt`/
`FiredCount`）及静默规则。重启恢复后：

* 未恢复的告警仍是同一条，**不会重复刷屏**（抑制计时从持久化的
  `LastFiredAt` 继续），也**不会丢失**；
* 静默继续生效直到其 `EndsAt`；
* 恢复后再次恶化依旧能重新触发。

## 5. 持久化语义

* 单一快照文件，JSON（`internal/store`），包含：版本、保存时间、算子聚合、
  去重集合 `SeenEvents`、各算子水位、全部告警、全部静默。
* 写入采用 **临时文件 → fsync → 原子 rename**：崩溃只会留下旧快照或新快照，
  不会出现写坏的半文件。
* 文件不存在（首次启动）等价于加载一个空快照，首启与重启走同一代码路径。
* `Engine.Save()` 可显式调用，`Close()` 会落盘；加载时做版本校验与 nil 容错。

## 6. 配置项

`model.Config`（默认值见 `DefaultConfig()`，启动时 `Validate`）：

| 配置项 | 含义 | 默认 |
|---|---|---|
| `LatenessTolerance` | 相对水位的迟到容忍；`EventTime >= wm-它` 即接受 | 30s |
| `DataLagThreshold` | 数据滞后阈值（严格大于才触发，必须 >0） | 60s |
| `TaskTimeout` | 任务运行超时阈值（严格大于才触发，必须 >0） | 5m |
| `AlertSuppressWindow` | 同一根因持续恶化时的再通知最小间隔（严格大于） | 10m |
| `EvaluationInterval` | 信息性配置；由调用方自行驱动 `Evaluate()` | 1s |

## 7. 测试覆盖与可复现性

`go test -race ./...` 覆盖需求要点，且用 `-v` 可看到关键事件与判定依据日志：

* `TestBasicAggregationAndDedup`：乱序、窗口内迟到、边界（闭区间）、过期显式
  标记、重复投递不二次计数；
* `TestConcurrentIngestConsistency`：16 goroutine × 200 事件并发 + 重投，
  总数与重复数确定；
* `TestOutOfOrderRunLifecycle`：失败先到、成功更晚到的终态纠正与恢复；
* `TestTimeoutAndLagThresholds`：三原因区分、严格大于的边界口径、data_lag
  恢复不影响 task_timeout；
* `TestClockRollbackStability`：时钟回拨不伪造/抹除结论、不重复触发；
* `TestAlertDedupSuppressRefire` / `TestSilence`：抑制窗口、静默（含通配与
  半开边界）、恢复后立即再触发；
* `TestRestartRecovery`：重启后聚合、去重、水位/过期口径、未恢复告警、抑制
  计时与静默全部延续，不刷屏不丢失；
* `store`/`alert` 包级测试：快照往返、缺失文件语义、恢复后抑制计数延续。
