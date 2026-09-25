package pipelineobs

import "time"

// EventKind 标识流水线运行事件的类型。
type EventKind string

const (
	// KindRecord 表示算子处理了一条数据记录（吞吐/延迟统计）。
	KindRecord EventKind = "record"
	// KindTaskStarted 表示周期任务开始运行。
	KindTaskStarted EventKind = "task_started"
	// KindTaskFinished 表示周期任务运行结束（成功或失败）。
	KindTaskFinished EventKind = "task_finished"
	// KindWatermark 表示数据水位推进（可选，缺省由记录事件推导）。
	KindWatermark EventKind = "watermark"
)

// Event 是接入引擎的一条运行事件。
//
// 时间语义：
//   - OccurredAt 为事件时间（数据产生时刻，业务时间）。
//   - 接入时刻由引擎时钟给出（处理时间），调用方无需填写。
type Event struct {
	// ID 为事件幂等标识。同一 ID 的重复投递只计数一次。
	ID string
	// Pipeline 与 Operator 共同定位统计归属。
	Pipeline string
	Operator string
	Kind     EventKind

	// OccurredAt 事件时间（UTC）。
	OccurredAt time.Time
	// Watermark 仅 KindWatermark 使用，表示算子当前水位。
	Watermark time.Time

	// TaskID 仅任务类事件使用，标识同一次任务运行。
	TaskID string
	// Failed / ErrorMessage 仅 KindTaskFinished 使用。
	Failed       bool
	ErrorMessage string

	// Bytes 可选，记录事件处理的数据字节数。
	Bytes int64
}

// FailureReason 区分“数据滞后 / 任务超时 / 执行失败 / 过期”等判定原因。
type FailureReason string

const (
	ReasonNone      FailureReason = ""          // 正常并入
	ReasonExpired   FailureReason = "expired"   // 超出迟到容忍窗口
	ReasonStale     FailureReason = "stale"     // 数据滞后
	ReasonTimeout   FailureReason = "timeout"   // 任务运行超时
	ReasonFailed    FailureReason = "failed"    // 任务执行失败
	ReasonDuplicate FailureReason = "duplicate" // 重复投递（不计数）
)

// Verdict 是单条事件接入后的判定结论。
type Verdict struct {
	EventID string
	// Accepted 为 true 表示事件被计入统计；false 表示被拒绝（重复或过期）。
	Accepted bool
	// Duplicate 为 true 表示是重复投递。
	Duplicate bool
	// Expired 为 true 表示超过迟到容忍窗口，仅标记不并入。
	Expired bool
	// WindowStart 事件时间落入的聚合窗口起点。
	WindowStart time.Time
	// Reason 判定原因（唯一口径）。
	Reason FailureReason
	// Detail 人类可读的判定依据。
	Detail string
}

// IngestResult 是一次接入的返回结果。
type IngestResult struct {
	Verdict Verdict
	// Alerts 本次接入新产生的告警（已做去重/静默过滤）。
	Alerts []Alert
	// Recovered 本次接入恢复的告警。
	Recovered []Alert
}
