package pipelineobs

import (
	"fmt"
	"time"
)

// snapshot 是引擎需要持久化的全部状态。JSON 序列化后原子落盘，
// 进程重启时整体恢复，保证告警、静默与去重信息跨重启延续。
type snapshot struct {
	// Version 用于持久化格式演进。
	Version int `json:"version"`
	// SavedAt 最近一次保存时间（处理时间）。
	SavedAt time.Time `json:"saved_at"`

	// Buckets 聚合窗口统计，键为 bucketKey(...)。
	Buckets map[string]*bucketState `json:"buckets"`
	// Tasks 在途任务，键为 taskID。
	Tasks map[string]*taskState `json:"tasks"`
	// Incidents 告警事件单（含已恢复的历史轮次），键为告警 Key。
	Incidents map[string]*incidentState `json:"incidents"`
	// Silences 静默规则，键为告警 Key；globalSilenceKey 表示全局静默。
	Silences map[string]silenceState `json:"silences"`

	// SeenEvents 已处理事件 ID -> 首次接入处理时间，用于幂等去重。
	SeenEvents map[string]time.Time `json:"seen_events"`
	// ExpiredEvents 被标记为过期的事件 ID -> 判定时间（只留痕不计数）。
	ExpiredEvents map[string]time.Time `json:"expired_events"`

	// Watermarks 各算子已确认的事件时间水位，键为 operatorKey(...)。
	Watermarks map[string]time.Time `json:"watermarks"`
	// LastIngest 各算子最近一次有效接入的处理时间（新鲜度判定用）。
	LastIngest map[string]time.Time `json:"last_ingest"`
	// EffectiveNow 单调有效的“当前时间”，抵御时钟回拨。
	EffectiveNow time.Time `json:"effective_now"`
}

// bucketState 是单个 (流水线, 算子, 窗口) 的聚合统计。
type bucketState struct {
	Pipeline    string    `json:"pipeline"`
	Operator    string    `json:"operator"`
	WindowStart time.Time `json:"window_start"`
	WindowEnd   time.Time `json:"window_end"`
	Closed      bool      `json:"closed"` // 水位越过窗口末端后关闭

	RecordCount int64 `json:"record_count"`
	ByteCount   int64 `json:"byte_count"`
	// LatencySumMS 处理延迟之和（OccurredAt -> 接入处理时间），毫秒。
	LatencySumMS int64 `json:"latency_sum_ms"`
	// MinEventTime / MaxEventTime 窗口内事件时间范围（零值表示尚无事件）。
	MinEventTime time.Time `json:"min_event_time"`
	MaxEventTime time.Time `json:"max_event_time"`

	TaskStarted  int64 `json:"task_started"`
	TaskFinished int64 `json:"task_finished"`
	TaskFailed   int64 `json:"task_failed"`

	UpdatedAt time.Time `json:"updated_at"`
}

// taskState 是一次任务运行的状态。
type taskState struct {
	TaskID   string `json:"task_id"`
	Pipeline string `json:"pipeline"`
	Operator string `json:"operator"`

	StartedAt    time.Time `json:"started_at"` // 事件时间
	FinishedAt   time.Time `json:"finished_at"`
	Failed       bool      `json:"failed"`
	ErrorMessage string    `json:"error_message"`
	Running      bool      `json:"running"`

	// TimeoutAlerted 标记本轮运行是否已产生过超时告警（抑制刷屏）。
	TimeoutAlerted bool `json:"timeout_alerted"`
	// FailAlerted 标记本轮运行是否已产生过失败告警。
	FailAlerted bool `json:"fail_alerted"`
}

// incidentState 是一条根因告警的持久化状态机。
type incidentState struct {
	Key      string        `json:"key"`
	Pipeline string        `json:"pipeline"`
	Operator string        `json:"operator"`
	Cause    Cause         `json:"cause"`
	Severity AlertSeverity `json:"severity"`

	State AlertState `json:"state"`

	FirstFiredAt time.Time `json:"first_fired_at"`
	LastFiredAt  time.Time `json:"last_fired_at"`
	RecoveredAt  time.Time `json:"recovered_at"`
	FireCount    int       `json:"fire_count"`

	// Notified 标记当前 firing 轮次是否已对外产生过一条有效告警。
	// 抑制窗口内重复条件成立只刷新 LastFiredAt，不再产生新告警。
	Notified bool `json:"notified"`
}

// silenceState 是一条静默规则。
type silenceState struct {
	// Until 为零值表示永久静默；否则到该处理时间为止。
	Until time.Time `json:"until"`
	Note  string    `json:"note"`
}

// globalSilenceKey 是全局静默规则在 Silences 中的键。
const globalSilenceKey = "*"

func newSnapshot() *snapshot {
	return &snapshot{
		Version:       snapshotVersion,
		Buckets:       map[string]*bucketState{},
		Tasks:         map[string]*taskState{},
		Incidents:     map[string]*incidentState{},
		Silences:      map[string]silenceState{},
		SeenEvents:    map[string]time.Time{},
		ExpiredEvents: map[string]time.Time{},
		Watermarks:    map[string]time.Time{},
		LastIngest:    map[string]time.Time{},
	}
}

// normalizeAfterLoad 补齐旧版本/空快照中可能为 nil 的 map，
// 并校验持久化格式版本，保证重启恢复后内存结构可直接使用。
func (s *snapshot) normalizeAfterLoad() error {
	if s.Version != snapshotVersion {
		return fmt.Errorf("snapshot: unsupported version %d (want %d)", s.Version, snapshotVersion)
	}
	if s.Buckets == nil {
		s.Buckets = map[string]*bucketState{}
	}
	if s.Tasks == nil {
		s.Tasks = map[string]*taskState{}
	}
	if s.Incidents == nil {
		s.Incidents = map[string]*incidentState{}
	}
	if s.Silences == nil {
		s.Silences = map[string]silenceState{}
	}
	if s.SeenEvents == nil {
		s.SeenEvents = map[string]time.Time{}
	}
	if s.ExpiredEvents == nil {
		s.ExpiredEvents = map[string]time.Time{}
	}
	if s.Watermarks == nil {
		s.Watermarks = map[string]time.Time{}
	}
	if s.LastIngest == nil {
		s.LastIngest = map[string]time.Time{}
	}
	return nil
}

// windowFloor 返回事件时间 t 所落入窗口的起点。
//
// 切分口径（唯一确定）：窗口按 Unix 纪元对齐，长度为 size；
// 窗口为左闭右开 [start, start+size)，边界事件属于“右侧”窗口。
func windowFloor(t time.Time, size time.Duration) time.Time {
	// 归一到 UTC 后按零时刻对齐切分；所有事件时间均晚于 Unix 纪元，
	// 因此 Truncate 与按纪元对齐完全等价，且边界口径由标准库唯一确定。
	if size <= 0 {
		size = time.Minute
	}
	return t.UTC().Truncate(size)
}

// bucketKey 生成聚合桶键。
func bucketKey(pipeline, operator string, windowStart time.Time) string {
	return fmt.Sprintf("%s|%s|%d", pipeline, operator, windowStart.UnixNano())
}

// operatorKey 生成算子键（水位/新鲜度使用）。
func operatorKey(pipeline, operator string) string {
	return pipeline + "|" + operator
}

// alertKey 生成告警根因键：pipeline|operator|cause。
func alertKey(pipeline, operator string, cause Cause) string {
	return fmt.Sprintf("%s|%s|%s", pipeline, operator, cause)
}

// ensureBucket 取或建指定窗口的聚合桶。
func (s *snapshot) ensureBucket(pipe, op string, start time.Time, size time.Duration) *bucketState {
	key := bucketKey(pipe, op, start)
	b, ok := s.Buckets[key]
	if !ok {
		b = &bucketState{
			Pipeline:    pipe,
			Operator:    op,
			WindowStart: start,
			WindowEnd:   start.Add(size),
		}
		s.Buckets[key] = b
	}
	return b
}
