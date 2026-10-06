package pipelineobs

import (
	"fmt"
	"strings"
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
	// SilenceRules 按范围生效的静默规则，键为规则 ID。
	SilenceRules map[string]silenceRule `json:"silence_rules"`
	// Silences 为 v1 持久化格式遗留字段；加载 v1 快照时迁移成 SilenceRules，
	// 新写入恒为空（自定义 JSON 解码完成迁移）。
	Silences map[string]silenceState `json:"silences,omitempty"`

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

	// LastProgressAt 为该任务最近一次“有进展”的处理时间（单调有效时间）：
	// 任务开始事件接入时置位；同算子此后任何被接受的记录/水位/任务事件
	// 都会刷新它。卡住判定只看它与 EffectiveNow 的差，无需任何新数据。
	LastProgressAt time.Time `json:"last_progress_at"`

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

	// Muted 标记当前 firing 轮次是否正被某条静默规则覆盖。
	// 只在 false->true（开始被静默）与 true->false（覆盖解除/到期，
	// 且问题仍在）两种跳变上驱动通知口径：进入静默时不补发；
	// 静默全部解除时复位 Notified，使持续中的问题能重新发出一条告警。
	Muted bool `json:"muted"`
}

// silenceState 是 v1 格式的静默规则（仅用于旧快照迁移）。
type silenceState struct {
	// Until 为零值表示永久静默；否则到该处理时间为止。
	Until time.Time `json:"until"`
	Note  string    `json:"note"`
}

// SilenceScope 标识一条静默规则的作用范围层级（口径唯一）。
type SilenceScope string

const (
	// ScopeGlobal 全局：静默所有告警（所有流水线/算子/原因）。
	ScopeGlobal SilenceScope = "global"
	// ScopePipeline 整条流水线：静默该流水线下所有算子、所有原因。
	ScopePipeline SilenceScope = "pipeline"
	// ScopeOperator 单个算子：静默该算子名下所有原因
	// （如算子名下所有任务失败、所有任务卡住）。
	ScopeOperator SilenceScope = "operator"
	// ScopeCause 单算子 + 单原因（含单次任务失败，由 TaskID 精确定位）。
	ScopeCause SilenceScope = "cause"
)

// silenceRule 是一条按范围生效的静默规则（持久化）。
//
// 范围口径（更宽的范围字段留空）：
//   - global:     Pipeline/Operator/Cause/TaskID 全空；
//   - pipeline:   Pipeline 非空，其余空；
//   - operator:   Pipeline+Operator 非空，Cause/TaskID 空；
//   - cause:      Pipeline+Operator+Cause 非空；TaskID 非空时进一步
//     精确到单次任务失败告警。
type silenceRule struct {
	ID       string       `json:"id"`
	Scope    SilenceScope `json:"scope"`
	Pipeline string       `json:"pipeline,omitempty"`
	Operator string       `json:"operator,omitempty"`
	Cause    Cause        `json:"cause,omitempty"`
	TaskID   string       `json:"task_id,omitempty"`
	// Until 为零值表示永久静默；否则到该处理时间为止（now >= Until 失效）。
	Until time.Time `json:"until"`
	Note  string    `json:"note,omitempty"`
}

func newSnapshot() *snapshot {
	return &snapshot{
		Version:       snapshotVersion,
		Buckets:       map[string]*bucketState{},
		Tasks:         map[string]*taskState{},
		Incidents:     map[string]*incidentState{},
		SilenceRules:  map[string]silenceRule{},
		Silences:      map[string]silenceState{},
		SeenEvents:    map[string]time.Time{},
		ExpiredEvents: map[string]time.Time{},
		Watermarks:    map[string]time.Time{},
		LastIngest:    map[string]time.Time{},
	}
}

// normalizeAfterLoad 补齐空快照中可能为 nil 的 map，并把旧持久化格式
// 迁移到当前版本，保证重启恢复后内存结构可直接使用。
func (s *snapshot) normalizeAfterLoad() error {
	switch s.Version {
	case 1:
		if err := s.migrateV1Silences(); err != nil {
			return err
		}
		s.Version = snapshotVersion
	case snapshotVersion:
		// 已是当前版本。
	default:
		return fmt.Errorf("snapshot: unsupported version %d (want <= %d)", s.Version, snapshotVersion)
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
	if s.SilenceRules == nil {
		s.SilenceRules = map[string]silenceRule{}
	}
	s.Silences = nil // 迁移完成后不再以旧格式写回。
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

// migrateV1Silences 把 v1 的键值静默规则迁移为 v2 的结构化范围规则。
//
// v1 只有两类键："*" 表示全局；其余为精确告警键 pipeline|operator|cause，
// 失败键形如 pipeline|operator|task_failed|<TaskID>。
func (s *snapshot) migrateV1Silences() error {
	rules := map[string]silenceRule{}
	for key, sil := range s.Silences {
		r, err := parseLegacySilenceKey(key, sil)
		if err != nil {
			return fmt.Errorf("snapshot: migrate v1 silence %q: %w", key, err)
		}
		rules[r.ID] = r
	}
	s.SilenceRules = rules
	return nil
}

// parseLegacySilenceKey 解析 v1 静默键（"*" 或 pipe|op|cause[|taskID]）。
func parseLegacySilenceKey(key string, sil silenceState) (silenceRule, error) {
	if key == "*" {
		return silenceRule{ID: silenceRuleID(ScopeGlobal, "", "", "", ""), Scope: ScopeGlobal,
			Until: sil.Until, Note: sil.Note}, nil
	}
	parts := strings.Split(key, "|")
	if len(parts) < 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return silenceRule{}, fmt.Errorf("unexpected legacy key shape")
	}
	pipe, op, cause := parts[0], parts[1], Cause(parts[2])
	if len(parts) > 3 {
		taskID := strings.Join(parts[3:], "|")
		if cause != CauseFailed {
			return silenceRule{}, fmt.Errorf("task-qualified key with cause %s", cause)
		}
		return silenceRule{
			ID: silenceRuleID(ScopeCause, pipe, op, cause, taskID), Scope: ScopeCause,
			Pipeline: pipe, Operator: op, Cause: cause, TaskID: taskID,
			Until: sil.Until, Note: sil.Note,
		}, nil
	}
	return silenceRule{
		ID: silenceRuleID(ScopeCause, pipe, op, cause, ""), Scope: ScopeCause,
		Pipeline: pipe, Operator: op, Cause: cause,
		Until: sil.Until, Note: sil.Note,
	}, nil
}
