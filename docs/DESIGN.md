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

## 3. 新鲜度、任务卡住与任务时效判定

四类异常使用四种**不同原因码**（`FailureReason` / 告警 `Cause`），绝不混淆。
它们可以分别独立成立、也可能同时成立，汇总与通知按原因码各算各的：

| 现象 | 原因码 | 判定依据（什么在往前走） | 边界口径 |
|---|---|---|---|
| 数据滞后 | `data_stale` / `ReasonStale` | `EffectiveNow − 该算子最近一次有效接入时间` | **≥ `StalenessThreshold` 即滞后** |
| 任务卡住（无进展） | `task_stuck` / `ReasonStuck` | `EffectiveNow − 该在途任务最近一次任何事件进展时间`，任务仍未结束 | **≥ `TaskStuckThreshold` 即卡住** |
| 任务运行超时 | `task_timeout` / `ReasonTimeout` | `算子事件时间水位 − 任务开始事件时间`，任务仍在运行 | **≥ `TaskTimeout` 即超时** |
| 任务执行失败 | `task_failed` / `ReasonFailed` | `task_finished` 事件带 `Failed=true` | 一次性事实 |

### 3.1 “任务卡住”语义（与数据滞后、执行失败的区别）

- **触发条件**：某条流水线的某个算子起了一个任务（收到 `task_started`）后，
  该任务长时间没有任何后续事件、也没有结束。这里的判定**只依赖真实处理时钟
  （`EffectiveNow`）往前走**——即使完全没有新数据进来、事件时间水位一动不动，
  只要值班周期调用 `ObserveTime()`（或任何接入顺带评估），到了阈值就必须明确
  判出 `task_stuck`，而不必“等后面有事件把时间推上去”。
- **进展锚点 `LastProgressAt`（处理时间）**：任务开始时置位；此后**同算子的
  任何“被接受”事件**（数据记录、显式水位、该算子的任务开始/结束）都把该算子
  名下所有仍在运行任务的锚点刷新为当前有效时间。重复、过期事件在更早的判定
  分支就被挡下，不会刷新锚点，因此不能制造“假进展”。
- **与数据滞后的区别**：数据滞后是“算子级、看最近一次接入”；卡住是“在途
  任务级、看该任务的最近进展、且要求任务未结束”。算子刚接入过（不滞后）也
  可能有一个更早起的任务一直没结束而卡住；反之只有记录、没有在途任务则永不
  会卡住。二者原因码不同，可同时 firing。
- **与任务超时的区别**：超时按**事件时间水位**判定（数据流要把水位推上去），
  卡住按**处理时间**判定（数据流可以完全不动）。任务水位一直不动、但真实时间
  在走，只会先/同时判卡住，不会误判超时；二者原因码不同，可同时 firing。
- **与执行失败的区别**：失败是 `task_finished(Failed=true)` 的一次性事实；
  卡住任务始终 `Running=true`、没有结束事件。
- **聚合口径**：卡住根因键按算子共享 `pipeline|operator|task_stuck`：该算子
  名下任一在途任务卡住即 firing；全部在途任务都恢复进展或结束后才恢复。
  评估时先按算子聚合“是否任一卡住”，再推进一次状态机，避免多任务处理顺序
  造成同一次评估内 firing→recovered→firing 抖动。
- **自动恢复 / 重新报警**：卡住任务重新有任何被接受事件（恢复进展），或收到
  正常结束事件，卡住即 `firing→recovered`；之后若再次长时间无进展，作为新的
  一轮恶化 `FireCount+1` 重新报出。
- **时钟回拨稳定**：锚点与判定都走单调有效时间 `EffectiveNow`，时钟被往回调
  不会让差值变小，因此卡住结论不会误恢复、也不会重复告警（见
  `TestTaskStuckClockRollbackStable`）。
- **重启延续**：锚点与卡住事件单随快照持久化。重启前已 firing 的卡住原样恢复，
  重新评估不重复刷屏；任务恢复进展后正常恢复。对没有锚点字段的旧版本快照，
  打开时做保守回填（见 §5.2）。

要点：

- **超时按事件时间水位评估**，与处理时钟快慢、回拨无关；迟到事件并入并推进
  水位后同样能触发超时（`TestTimeoutIsEventTimeDriven`）。这让结论只由
  数据流决定，可复现。
- **数据滞后与任务卡住基于单调有效处理时间**：回拨不会让其误恢复，恢复前进
  也不会误重复（`TestClockRollbackStability`、`TestTaskStuckClockRollbackStable`）。
- 没有新事件时可周期性调用 `Engine.ObserveTime()`，仅推进观察时刻并重评
  卡住/滞后/超时；接入新事件后会顺带评估该算子。`TestTaskStuckDetectedWithoutEvents`
  固定了“零事件、只推进时钟也能判卡住”以及“阈值前一刻不报、恰好到阈值当次必报”。

---

## 4. 告警：触发 / 去重抑制 / 静默 / 恢复

### 4.1 根因键（同一根因共享一个状态机）

- 数据滞后、任务卡住、任务超时：`pipeline|operator|cause`。
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

### 4.3 按范围静默（Silence）

静默用于值班维护：一次压住**某条流水线 / 某个算子名下一整类问题**，而不是
一个任务一个任务去点。

#### 4.3.1 范围层级与优先级（口径唯一）

静默规则有四个层级，宽层字段必须留空、窄层字段必须逐层给齐：

| 层级 | API | 覆盖范围 |
|---|---|---|
| 全局 | `Silence("","","",...)` | 所有流水线 / 算子 / 原因 |
| 整条流水线 | `SilencePipeline(pipe, ...)` | 该流水线下所有算子、所有原因 |
| 单个算子 | `SilenceOperator(pipe, op, ...)` | 该算子名下**所有原因**（含所有任务失败、所有任务卡住） |
| 单原因（可精确到单次任务） | `Silence(pipe,op,cause,...)` / `SilenceFailure(pipe,op,taskID,...)` | 仅该算子该原因；带 `TaskID` 时只覆盖那一次任务失败 |

- **是否生效只有一个布尔结论：“当前事件单是否被任一未到期规则覆盖”。**
  多种范围同时命中时不产生冲突或“有的生效有的不生效”——只要有任意一层覆盖，
  通知就被压住；宽层规则天然覆盖其名下所有更细问题。规则按确定性 ID 存储
  （`global` / `pipeline:<p>` / `operator:<p>|<o>` / `cause:<p>|<o>|<c>` /
  `task:<p>|<o>|task_failed|<id>`），同范围重复添加幂等。解除分别提供
  `RemoveSilence*` 与按 ID 的 `RemoveSilenceRule`；`ListSilences()` 可查。
- 静默只压“对外通知”，**内部状态机照常 firing/恢复并持久化**：静默期内
  已在报的问题保持 firing，静默期内新冒出的同类/名下问题也照样建档为 firing，
  只是 `ActiveAlerts()` 仍能看到、`IngestResult.Alerts` / `ObserveTime` 不返回。

#### 4.3.2 到期 / 解除后重新报警的规则（不因“以前报过”就永久静音）

每个事件单除 `Notified` 外另持久化一个 **`Muted`（当前是否被覆盖）** 位，
由统一“通知闸门”按**覆盖跳变**驱动：

- 未被覆盖且本轮未通知：发出这一轮唯一一条告警，置 `Notified`；
- 进入覆盖（`false→true`）：只置 `Muted`，不发、也不补发；
- 覆盖解除（`true→false`，含规则到期或被手动删除）：**复位 `Notified`**，
  于是只要问题仍 firing，当次评估立即重新发出一条告警；
- 若问题在静默期内已恢复、之后又重新恶化，则作为新一轮（`FireCount+1`），
  静默已解除时正常报出——两条路径都不会因为“历史上报过一次”而不出声。

到期边界口径与旧版本一致：观察时刻 **`now >= Until` 规则立即失效**。每次
告警评估开始时统一惰性清理到期规则，因此“到期”与“重新报警”在同一次
`ObserveTime()` / 接入内完成（`TestSilenceExpiryReAlertsPersistingProblem`、
`TestSilenceManualRemoveReAlerts`、`TestSilencePriorityIsUniform`、
`TestOperatorSilenceCoversWholeClass`、`TestPipelineSilenceCoversAllOperators`、
`TestCauseSilenceDoesNotLeak`）。`ttl=0` 表示永久静默。

> 手动解除本身不立即下发通知；由下一次评估（周期 `ObserveTime` 或下一条
> 事件接入）在同一口径下补发，保证“是否报警”始终只由评估状态机决定。

### 4.4 重启后的行为（`TestRestartContinuesAlertsAndDedup`、`TestStuckRestartContinuesWithoutSpam`）

- 未恢复（`firing`）的告警（含任务卡住）原样恢复，重新评估时**不重复发通知**。
- 静默规则原样恢复并继续生效/到期。
- 已处理 / 已过期事件 ID 集合恢复，历史事件重放一律判重，统计不变。

---

## 4A. 运行健康汇总（只读，与判定/活动告警严格同源）

`Engine.HealthSummary()` 返回 `[]PipelineHealth`，按流水线（再按算子）排序，
值班可一眼看清每条流水线、每个算子当前是否卡住、是否数据滞后、有几条在报警：

- `OperatorHealth{ Pipeline, Operator, Stuck, Stale, FiringCount, Causes }`；
- `PipelineHealth{ Pipeline, Stuck, Stale, FiringCount, Operators }`，
  流水线级标志为名下算子的“或”聚合，`FiringCount` 为求和。

一致性口径（唯一来源）：

1. 所有标志与计数**只从当前 `StateFiring` 的事件单派生**，与
   `ActiveAlerts()`、逐条接入的判定结果是同一份持久化状态、同一把锁下的快照；
   因此 `Σ FiringCount == len(ActiveAlerts())` 恒成立，乱序、延迟到达、
   时钟回拨、重启恢复后都不会各说各话（`TestHealthSummaryMatchesActiveAlerts`、
   `TestHealthSummaryConsistentAcrossRestart`）。
2. 算子全集 = 曾出现过窗口统计（`Buckets`）或任务（`Tasks`）的算子；当前没有
   任何 firing 告警的算子也会以健康形态（`Stuck=false, Stale=false,
   FiringCount=0`）出现，避免“没消息就不知道它在不在”。
3. 该视图纯只读，不推进任何状态、不产生告警、不刷盘。

---

## 5. 持久化语义

- 接口：`Store{ Save(*snapshot); Load() *snapshot }`；本地实现 `FileStore`。
- 格式：单个 JSON 快照文件（带 `version` 字段，**当前版本 2**；加载时支持
  v1→v2 自动迁移并补齐 map）。
- 写入协议：**写临时文件 → `fsync` → 原子 `rename`**。崩溃时只会看到旧的或
  新的完整快照，不会读到半截文件。
- 写入时机：每次成功接入、`ObserveTime` 后以及增删静默规则后整体覆写；
  `Close` 再刷一次。测试规模数据量下足够；store 传 `nil` 可纯内存运行。
- 快照内容：窗口聚合、任务（含卡住判定的进展锚点 `LastProgressAt`）、告警
  事件单（含历史轮次、`Notified` 与静默覆盖位 `Muted`）、**按范围的静默规则
  `SilenceRules`**、已处理/已过期事件 ID、各算子水位与最近接入时间、`EffectiveNow`。
- 启动时若 `EffectiveNow` 落后于当前时钟则追平时钟，保证重启后时间单调。

### 5.1 v1 → v2 静默规则迁移

v1 的静默是 `map[告警键]silenceState`，只有 `"*"`（全局）与精确键
`pipeline|operator|cause[|taskID]` 两类。加载 v1 快照时逐键解析为 v2 的
结构化范围规则（全局 / 单原因 / 单次任务失败），规则语义与到期时间保持不变；
迁移后清空旧字段并以 v2 写回。见 `TestV1SnapshotSilenceMigration`。

### 5.2 在途任务进展锚点的重启回填

旧版本快照中的任务没有 `LastProgressAt`。`Open` 恢复时对仍在运行且锚点为零的
任务做一次性保守回填：

- 若该算子重启前已有 **firing 的卡住事件单**，则不回填——保持零锚点，事件单
  经持久化原样延续（不丢、不刷屏），其“卡住”结论以已持久化状态为准；
- 否则锚点取该算子最近一次有效接入处理时间（再缺省取当前有效时间），避免
  “重启瞬间立即误报卡住”，同时持续无进展超过阈值后仍能正常判出。

v2 快照本身持久化了锚点，正常路径不触发回填。

---

## 6. 配置项（`Config`）

| 配置 | 说明 | 默认值 |
|---|---|---|
| `WindowSize` | 聚合窗口长度，按事件时间、纪元对齐切分 | `1m` |
| `AllowedLateness` | 迟到容忍窗口；`事件时间 >= 水位−该值` 并入旧窗口，更早的判过期 | `30s` |
| `StalenessThreshold` | 数据新鲜度阈值；`有效现在 − 最近接入 >= 该值` 判数据滞后 | `2m` |
| `TaskStuckThreshold` | 任务无进展阈值；`有效现在 − 在途任务最近进展 >= 该值` 判卡住；**设为 0 关闭卡住判定** | `3m` |
| `TaskTimeout` | 任务超时阈值；`事件时间水位 − 任务开始 >= 该值` 且仍运行判超时 | `5m` |
| `SuppressWindow` | 告警抑制窗口配置项；状态机口径为同一 firing 轮次恒只通知一条 | `10m` |

所有阈值均要求非负（`WindowSize` 非法时 `Open` 直接报错；
`TaskStuckThreshold` 为 0 时仅关闭卡住判定，其余判定不受影响）。

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
go test -v -run 'TestTaskStuck|TestSilence|TestHealthSummary|TestV1' .
                                      # 只看本次新增行为（卡住/范围静默/汇总/迁移）
cd cmd/demo
go run .                              # 首次运行，生成 demo-state.json
go run .                              # 再次运行 = 模拟进程重启恢复（卡住不刷屏、不丢）
rm -f demo-state.json                 # 回到全新状态
```

本次新增行为的重点自动化用例：

- **完全没有新事件、只推进真实时钟也能判卡住**、阈值前一刻不报/恰好到阈值
  当次必报：`TestTaskStuckDetectedWithoutEvents`；
- 卡住与滞后/超时/失败四类原因独立区分、可并存：
  `TestTaskStuckDistinctFromStaleAndTimeout`；
- 恢复进展 / 正常结束自动恢复、之后再卡住重新报（`FireCount` 递增）：
  `TestTaskStuckRecoversOnProgressAndCanRearm`；
- 时钟回拨结论不抖、不误恢复、不重复：`TestTaskStuckClockRollbackStable`；
- 按范围静默（整算子一整类、整条流水线、单原因不泄漏、多层同时命中口径唯一）：
  `TestOperatorSilenceCoversWholeClass`、`TestPipelineSilenceCoversAllOperators`、
  `TestCauseSilenceDoesNotLeak`、`TestSilencePriorityIsUniform`；
- 静默到期 / 手动解除后只要问题仍在或再恶化就重新报警：
  `TestSilenceExpiryReAlertsPersistingProblem`、`TestSilenceManualRemoveReAlerts`；
- 重启后卡住延续不刷屏、可恢复、可再报，静默规则延续：
  `TestStuckRestartContinuesWithoutSpam`；
- v1 快照静默规则迁移：`TestV1SnapshotSilenceMigration`；
- 健康汇总与活动告警严格对账（含乱序恢复后、重启后）：
  `TestHealthSummaryMatchesActiveAlerts`、`TestHealthSummaryConsistentAcrossRestart`。

原有用例（跟着数据推进走的滞后/超时判定、迟到/过期、去重、并发乱序、
静默抑制、重启去重等）全部保留且一个不回归，`go test ./...` 与
`-race -count=N` 均通过。

单测在 `-v` 日志中逐条打印：事件 ID/类型/事件时间、是否接受/重复/过期、
水位推进、窗口归属、告警触发/恢复/静默（MUTED/UNMUTED）及判定依据，
并对卡住、范围静默、到期重报、健康对账等业务打印“输入 → 判定依据”行，
便于按类型追溯覆盖了哪些业务。

## 9. 与原有能力的兼容范围

- 事件模型、窗口/迟到/过期、去重、数据滞后、任务超时/失败、聚合视图、
  `ObserveTime` 等既有行为与 API **完全保留**，判定口径不变。
- 卡住判定为**纯增量**：新原因码 `task_stuck` / `ReasonStuck`、新配置项
  `TaskStuckThreshold`（默认 `3m`；显式配 `0` 可关闭）。不配置新阈值时使用
  默认值即自动获得卡住能力，无需改调用方。
- 静默由 v1 的“全局/精确键”升级为“按范围规则”，但 `Silence(pipeline,
  operator, cause, ttl, note)` 与 `SilenceFailure(...)` 的**调用签名保持不变**，
  旧调用（含全空 = 全局）语义等价；新增 `SilencePipeline` / `SilenceOperator`
  与对应解除、`ListSilences()`。旧快照自动迁移（§5.1）。
- `HealthSummary()` 为新增只读接口，不影响任何既有接口。
