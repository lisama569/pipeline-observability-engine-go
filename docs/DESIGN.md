# 流水线可观测性引擎 — 设计与语义说明

本组件是一个**纯进程内**的流水线遥测接入与判定引擎：用协程模拟多条数据流水线
并发上报运行事件，用本地 JSON 快照模拟持久化，不依赖任何外部服务、网络资源或
真实凭据。所有结论都只依赖「事件自身携带的事件时间」「可注入的处理时钟」与
「已持久化的状态」，因此在并发、乱序、时钟回拨与进程重启场景下保持
**确定、一致、可复现、可解释**。

- 语言：Go（仅标准库）
- 入口包：仓库根包 `pipelineobs`
- 演示程序：`cmd/demo`（`go run ./cmd/demo`，再次运行即模拟重启恢复）
- 测试：`go test ./...`（建议加 `-race`；测试日志打印每条关键事件与判定依据）

---

## 1. 事件模型

### 1.1 事件类型（`EventKind`）

| 事件类型 | 含义 | 关键字段 |
|---|---|---|
| `record` | 算子处理了一条数据记录，用于吞吐 / 字节 / 处理延迟聚合 | `Bytes` |
| `task_started` | 周期任务开始运行 | `TaskID`、`OccurredAt` |
| `task_finished` | 周期任务结束（成功 / 失败） | `TaskID`、`Failed`、`ErrorMessage` |
| `watermark` | 显式推进数据水位（缺省也可由记录事件自动推导） | `Watermark` |

每个事件由 `(Pipeline, Operator)` 归属到某条流水线的某个算子；`ID` 是全局
**幂等标识**，用于重复投递去重。

### 1.2 双时间语义（这是全部判定可复现的基础）

- **事件时间（Event Time）**：`Event.OccurredAt`，数据真实产生的业务时间。
  窗口切分、迟到/过期判定、任务超时判定全部以事件时间为准。
- **处理时间（Processing Time）**：事件进入引擎时由 `Clock` 给出，调用方不填写。
  仅用于数据新鲜度（多久没接到数据）、告警触发/恢复时间戳与持久化时间戳。

`Clock` 是可注入接口：生产用 `SystemClock`，测试/演示用 `MockClock`
（可任意 `Set`，包括回拨）。

### 1.3 水位（Watermark）与单调时钟

- 每个算子维护一个事件时间水位 `Watermarks[pipeline|operator]`，取「历史已见
  事件时间 / 显式水位」的最大值，**单调不减**：乱序、迟到、处理时钟回拨都不会
  让它倒退。
- 引擎另维护一个 `EffectiveNow`：处理时钟回拨时有效时间保持为历史最大值，
  不随后退；时钟恢复后从真实时刻继续。这保证「数据滞后」这类基于处理时间的
  判定在回拨期间不会被错误恢复、也不会重复告警。

---

## 2. 窗口、迟到与过期规则

### 2.1 窗口切分（边界口径唯一）

- 窗口大小 `WindowSize`，按 **Unix 纪元对齐**（实现上对 UTC 时间做
  `Truncate(WindowSize)`，对所有晚于 1970 年的时间与纪元对齐等价）。
- 窗口为**左闭右开** `[start, start+WindowSize)`；落在边界（恰好整分）的事件
  归属「右侧」新窗口，不存在二义性。

### 2.2 去重 / 迟到 / 过期判定（按固定优先级）

事件进入后依次判定，结论互斥且唯一：

1. **重复投递（幂等去重）**
   - 若 `ID` 已在「已处理集合」或「已过期集合」中，立即判 `duplicate`：
     不计数、不改水位、不触发任何告警。
   - 对新 ID 采用**「插入即占座」**：持互斥锁检查后立即写入集合，因此同一 ID
     的两个并发投递中恰好一个生效（`-race` + 重复运行验证）。
2. **超窗过期（明确标记，而非静默丢弃）**
   - 容忍下界 = `当前水位 − AllowedLateness`。
   - 事件时间 **早于**容忍下界 ⇒ 判 `expired`：从已处理集合移入「过期集合」
     留痕，**不并入任何统计、不推进水位**；`IngestResult.Verdict` 中带
     `Expired=true`、`Reason=expired` 及人类可读依据（含事件时间、水位、下界）。
   - 过期事件若被重放，仍保持「曾经过期」的结论（判重），不会被重新计数。
3. **迟到但在容忍窗口内**
   - 事件时间早于当前水位但 **≥ 容忍下界** ⇒ 正常并入其事件时间所属的旧窗口，
     统计如实更新，水位保持不变。
   - **边界口径：事件时间恰好等于容忍下界时算「在容忍内」**（比较采用
     `eventTime >= watermark − AllowedLateness`），用例 `TestLateWithinToleranceAndExpired`
     固定了这一行为。
4. **准时事件**：正常并入，并把水位推进到该事件时间。

### 2.3 窗口关闭

水位一旦 `>= 窗口右端`，该窗口标记为 `Closed`。关闭后的窗口在
`AllowedLateness` 内仍可被迟到事件改写（这是允许的），只有超出容忍窗口的
事件才被标记过期。窗口不会被物理删除，快照中可追溯。

### 2.4 聚合指标（`BucketView`）

每个 `(流水线, 算子, 窗口)` 聚合：记录数、字节数、平均处理延迟
（处理时间 − 事件时间）、任务开始/结束/失败次数、窗口内事件时间范围、关闭标记。
所有计数为可交换、可结合的加法，故并发与乱序不影响最终值。

---

## 3. 新鲜度与任务时效判定

四类异常使用四种**不同原因码**（`FailureReason` / 告警 `Cause`），绝不混淆：

| 现象 | 原因码 | 判定依据 | 边界口径 |
|---|---|---|---|
| 数据滞后 | `data_stale` / `ReasonStale` | `EffectiveNow − 该算子最近一次有效接入时间` | **≥ `StalenessThreshold` 即滞后** |
| 任务运行超时 | `task_timeout` / `ReasonTimeout` | `算子事件时间水位 − 任务开始事件时间`，任务仍在运行 | **≥ `TaskTimeout` 即超时** |
| 任务卡死（无进展） | `task_stuck` | `EffectiveNow − 该任务所属算子最近一次有效事件的处理时间`，任务仍在运行 | **≥ `StuckThreshold` 即卡死** |
| 任务执行失败 | `task_failed` / `ReasonFailed` | `task_finished` 事件带 `Failed=true` | 一次性事实 |

要点：

- **超时按事件时间水位评估**，与处理时钟快慢、回拨无关；迟到事件并入并推进
  水位后同样能触发超时（`TestTimeoutIsEventTimeDriven`）。这让结论只由
  数据流决定，可复现。
- **数据滞后基于单调有效处理时间**：回拨不会让滞后误恢复，恢复前进也不会
  让滞后误重复（`TestClockRollbackStability`）。
- 没有新事件时可周期性调用 `Engine.ObserveTime()`，仅推进观察时刻并重评
  滞后/超时/卡死；接入新事件后会顺带评估该算子的滞后状态。

### 3.1 任务卡死（`task_stuck`）语义

卡死针对的是「任务起了之后算子彻底静默」的场景：没有任何新数据、没有结束
事件，**只有真实时钟在走**。它与滞后、超时的区别：

- **数据滞后**只要求「算子没新接入」，不要求有在途任务；**卡死**要求存在
  仍在运行的任务，是任务级结论。
- **超时**由事件时间水位驱动（数据流还在走、只是任务跑得久）；**卡死**由
  处理时间驱动（数据流本身停了）。同一任务可以同时超时且卡死，两条事件单
  各自独立。

判定细节：

- 每个在途任务记录 `LastActivityAt`：任务开始事件的处理时间为初始值，之后
  该算子**任何一条有效接入事件**（含记录、水位、其他任务事件）都刷新它。
- 条件：`任务在运行 且 EffectiveNow − LastActivityAt ≥ StuckThreshold`。
  边界口径唯一：**恰好等于阈值那一次即判卡死**（`TestStuckFiresOnWallClockOnly`）。
- 同一算子多个在途任务共享一条 `pipeline|operator|task_stuck` 事件单：
  任一任务卡死即成立，告警消息列出全部卡死任务 ID；结论按算子聚合，
  与任务遍历顺序无关。
- **时钟回拨不抖**：`EffectiveNow` 单调不减，无进展时长不会回退，回拨期间
  不误恢复、不重复告警（`TestStuckStableUnderClockRollback`）。
- **重启延续**：`LastActivityAt` 与事件单都持久化，重启后未恢复的卡死
  原样保留，不丢、不重复刷屏（`TestStuckSurvivesRestart`）。
- **自动恢复**：任务重新有产出（算子任一有效事件）或正常结束时，卡死结论
  自动恢复；之后再次卡住会开启新一轮重新告警
  （`TestStuckRecoversOnProgressAndFinish`）。
- **兼容开关**：`StuckThreshold <= 0`（含旧配置零值）时卡死判定整体关闭，
  行为与旧版完全一致（`TestStuckDisabledWhenThresholdZero`）。

---

## 4. 告警：触发 / 去重抑制 / 静默 / 恢复

### 4.1 根因键（同一根因共享一个状态机）

- 数据滞后、任务超时：`pipeline|operator|cause`。
- 任务失败：`pipeline|operator|task_failed|<TaskID>`。任务实例是不可变事实，
  每个实例独立一条事件单，因此「同一任务失败」重启/重放不重复，而「下一个
  任务失败」可再次触发。

### 4.2 状态机与抑制口径

每个根因是一个持久化的两态状态机：`firing ⇄ recovered`。

- 条件首次成立，或 `recovered` 后再次成立：开启**新一轮** `firing`，
  `FireCount++`，`FirstFiredAt` 更新。
- **同一轮 `firing` 期间，无论条件成立多少次、是否跨越 `SuppressWindow`，
  对外只产生一条有效告警**（`Notified` 位控制）。`SuppressWindow` 作为配置项
  保留；当前口径选择「未恢复即同一轮次」，比「每抑制窗口刷一条」更严格，
  彻底杜绝刷屏。
- 条件不再成立：`firing → recovered`，产生一条恢复通知。
- **恢复后再次恶化**：自动回到第 2 步，允许重新触发（`TestReWorsenAfterRecovery`）。
- 任务失败是一次性事件，不存在恢复；其「不重复」由 `Notified` 位 +
  任务上的 `FailAlerted` 位共同保证。

### 4.3 静默（Silence）

静默只抑制「对外通知」，**不影响状态机推进与持久化**：静默期间触发/持续的
告警在快照里仍是 `firing`，重启后延续。

**规则形态**（匹配口径唯一确定）：

- `Silence(pipeline, operator, cause, ttl, note)`：逐根因精确静默；
  全空参数表示**全局静默**。`SilenceFailure(...)` 精确静默某次任务失败。
- `SilenceScoped(SilenceScope{Pipeline, Operator, Cause, TaskID}, ttl, note)`：
  **按范围静默**。空字段为通配，非空字段全部相等才命中：
  - 仅 `Pipeline`：静默整条流水线的全部问题；
  - `Pipeline+Operator`：静默该算子名下全部问题（各 cause、各任务实例），
    **已正在报的与静默期内新冒出来的同类问题都覆盖**；
  - 再叠加 `Cause` / `TaskID`：进一步收窄到某一类问题 / 某次任务失败。
  - 范围相同则规则 ID 相同，重复添加为幂等覆盖；`RemoveSilenceRule(id)`
    手动解除；`ListSilences()` 查看全部未到期规则。

**多范围同时命中**：效果为并集——任一未到期规则命中即静默，不存在
「有的生效有的不生效」。需要解释「被哪条规则压住」时，取具体程度最高
（精确键 > 匹配字段多者）、并列按规则 ID 字典序，结果唯一确定
（`TestOverlappingScopesDeterministic`）。

**到期与重报**：

- `ttl=0` 永久静默，否则从当前有效时间起持续 `ttl`；边界口径为观察时刻
  `now >= Until` 即失效（惰性清理）。
- **静默到期或被手动解除后，只要问题仍在 firing，下一次评估必须重新发出
  一条告警**——即使该 firing 轮次在静默前已经通知过一次。实现上事件单
  记录 `Silenced` 位：上次评估处于静默覆盖、本次不再覆盖且条件仍成立，
  即补发（同一轮次，FireCount 不变）。「又来一轮新的恶化」则按状态机
  正常开启新一轮、FireCount+1。
- 静默期内发生且被压住的任务失败，在静默解除后的下一次评估补发
  （`TestOperatorScopeSilenceCoversTaskFailures`）。

### 4.4 重启后的行为（`TestRestartContinuesAlertsAndDedup`）

- 未恢复（`firing`）的告警原样恢复，重新评估时**不重复发通知**。
- 静默规则（含范围静默）原样恢复并继续生效/到期
  （`TestScopedSilenceSurvivesRestart`）。
- 已处理 / 已过期事件 ID 集合恢复，历史事件重放一律判重，统计不变。

---

## 5. 持久化语义

- 接口：`Store{ Save(*snapshot); Load() *snapshot }`；本地实现 `FileStore`。
- 格式：单个 JSON 快照文件（带 `version` 字段，当前版本 1；加载时校验版本并
  补齐 map）。
- 写入协议：**写临时文件 → `fsync` → 原子 `rename`**。崩溃时只会看到旧的或
  新的完整快照，不会读到半截文件。
- 写入时机：每次成功接入、`ObserveTime` 后以及增删静默规则后整体覆写；
  `Close` 再刷一次。测试规模数据量下足够；store 传 `nil` 可纯内存运行。
- 快照内容：窗口聚合、任务（含 `LastActivityAt`）、告警事件单（含历史轮次、
  `Notified`、`Silenced`）、静默规则（精确键与范围规则）、已处理/已过期
  事件 ID、各算子水位与最近接入时间、`EffectiveNow`。
- 启动时若 `EffectiveNow` 落后于当前时钟则追平时钟，保证重启后时间单调。
- **旧版快照兼容**：格式版本仍为 1，新字段均为 JSON 可选字段；旧快照中的
  逐根因静默条目在加载时自动迁移为「精确键匹配」规则，语义不变
  （`normalizeAfterLoad`）。`StuckThreshold` 在旧配置中为零值，卡死判定
  自动关闭，因此旧配置 + 旧快照的行为与升级前完全一致。

---

## 5.1 运行健康汇总（`Engine.Health()`）

只读视图，按「流水线 → 算子」两级聚合，供值班一眼看清当前状态：

- 算子级：是否卡死（`Stuck`）、是否数据滞后（`Stale`）、在途任务数、
  活动告警数、firing 原因集合；流水线级为各算子的并集/求和。
- **一致性口径**：汇总完全由已持久化的事件单（firing 状态）与任务状态
  推导，与 `ActiveAlerts()`、逐条接入的判定结果严格对得上；乱序、延迟
  到达、重启恢复后都不各说各话（`TestHealthMatchesActiveAlerts`、
  `TestHealthConsistentAfterOutOfOrderAndRestart`）。
- 汇总本身不推进时间、不修改状态；要刷新时效类结论（滞后/超时/卡死）
  先调用 `ObserveTime()` 再看汇总。

---

## 6. 配置项（`Config`）

| 配置 | 说明 | 默认值 |
|---|---|---|
| `WindowSize` | 聚合窗口长度，按事件时间、纪元对齐切分 | `1m` |
| `AllowedLateness` | 迟到容忍窗口；`事件时间 >= 水位−该值` 并入旧窗口，更早的判过期 | `30s` |
| `StalenessThreshold` | 数据新鲜度阈值；`有效现在 − 最近接入 >= 该值` 判数据滞后 | `2m` |
| `TaskTimeout` | 任务超时阈值；`事件时间水位 − 任务开始 >= 该值` 且仍运行判超时 | `5m` |
| `StuckThreshold` | 卡死阈值；`有效现在 − 算子最近有效事件 >= 该值` 且有在途任务判卡死；`<=0` 关闭 | `3m` |
| `SuppressWindow` | 告警抑制窗口配置项；状态机口径为同一 firing 轮次恒只通知一条 | `10m` |

所有阈值均要求正数（`WindowSize` 非法时 `Open` 直接报错）。

---

## 7. 并发模型与可复现性

- `Engine` 内部用一把 `sync.Mutex` 串行化状态变更（接入 → 判定 → 聚合 →
  告警评估 → 刷盘），对外 `Ingest` 可被任意多 goroutine 并发调用。
- 临界区内只做内存计算与一次本地文件写，无网络 / 阻塞 IO 外部依赖。
- 可复现性来源：
  1. 聚合只依赖事件时间与加法；
  2. 水位与 `EffectiveNow` 单调，乱序/回拨不改写历史；
  3. 告警是否产生只取决于持久化状态机位（`State`、`FireCount`、`Notified`）；
  4. 测试通过 `MockClock` 固定全部时间，同一事件序列重复执行得到逐字段一致的
     结论（`TestDeterministicUnderRepeatedRun`，并以 `-race -count=N` 反复验证）。

## 8. 本地验证方法

```bash
go test -race -count=5 ./...          # 并发/乱序/重复/重启 全套单测
go run ./cmd/demo                     # 首次运行，生成 demo-state.json
go run ./cmd/demo                     # 再次运行 = 模拟进程重启恢复
rm -f demo-state.json                 # 回到全新状态
```

单测在 `-v` 日志中逐条打印：事件 ID/类型/事件时间、是否接受/重复/过期、
水位推进、窗口归属、告警触发/恢复及判定依据，便于按类型追溯失败原因。
