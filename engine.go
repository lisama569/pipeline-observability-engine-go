package pipelineobs

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Config 是引擎配置。所有判定阈值在此集中声明，口径唯一。
type Config struct {
	// WindowSize 聚合窗口大小（按事件时间切分，按 Unix 纪元对齐）。
	WindowSize time.Duration
	// AllowedLateness 迟到容忍：事件时间 >= 水位 - AllowedLateness
	// 的迟到事件并入既有窗口；更早的事件标记 expired（不静默丢弃）。
	AllowedLateness time.Duration

	// StalenessThreshold 数据新鲜度阈值：某算子距最近一次有效接入
	// （按单调有效时间）达到该阈值即判定数据滞后（>= 边界成立）。
	StalenessThreshold time.Duration
	// TaskTimeout 任务超时阈值：事件时间水位 - 任务开始事件时间
	// 达到该值时，仍在运行的任务判定超时（>= 边界成立）。
	TaskTimeout time.Duration

	// TaskStuckThreshold 任务无进展（卡死）阈值：任务起后，按单调有效的
	// 处理时间计，距该任务最近一次任何事件进展（数据记录、显式水位、
	// 任务自身的开始/结束事件）达到该值仍未结束，即判定任务卡住（>= 边界成立）。
	// 与数据滞后的区别：只要求真实时钟往前走、完全没有任何新事件也能判出；
	// 与超时的区别：不依赖事件时间水位推进。
	TaskStuckThreshold time.Duration

	// SuppressWindow 告警抑制窗口（保留配置项；当前状态机口径为同一
	// firing 轮次恒只通知一条，恢复后重新恶化方可再次触发）。
	SuppressWindow time.Duration
}

// DefaultConfig 返回一组可用的默认阈值。
func DefaultConfig() Config {
	return Config{
		WindowSize:         time.Minute,
		AllowedLateness:    30 * time.Second,
		StalenessThreshold: 2 * time.Minute,
		TaskTimeout:        5 * time.Minute,
		TaskStuckThreshold: 3 * time.Minute,
		SuppressWindow:     10 * time.Minute,
	}
}

// Logger 是引擎内部可观测日志接口（测试用它打印关键事件与判定依据）。
type Logger interface {
	Printf(format string, args ...any)
}

// Engine 是进程内流水线可观测性引擎。零值不可用，请用 Open。
type Engine struct {
	cfg   Config
	clock Clock
	store Store
	log   Logger

	mu     sync.Mutex
	state  *snapshot
	closed bool
}

// Open 打开引擎：从 store 恢复持久化状态（若有），无历史则初始化。
func Open(ctx context.Context, cfg Config, clock Clock, store Store) (*Engine, error) {
	if cfg.WindowSize <= 0 {
		return nil, errors.New("engine: WindowSize must be positive")
	}
	if clock == nil {
		clock = SystemClock{}
	}
	e := &Engine{cfg: cfg, clock: clock, store: store, state: newSnapshot()}
	if store != nil {
		s, err := store.Load(ctx)
		if err != nil {
			return nil, fmt.Errorf("engine: load state: %w", err)
		}
		if s != nil {
			e.state = s
		}
	}
	// 若恢复出来的 EffectiveNow 落后于当前时钟，则追平时钟。
	if now := e.clock.Now(); now.After(e.state.EffectiveNow) {
		e.state.EffectiveNow = now
	}
	// 回填在途任务的进展锚点（旧版本快照没有该字段）。
	// 若该算子重启前已有 firing 的卡住事件单，则它是“重启前就卡住、尚未恢复”
	// 的状态：保持零锚点不动，事件单经持久化延续，不会因重启重复刷屏、也不会丢；
	// 否则以最近有效接入处理时间（缺失则当前有效时间）作保守锚点，避免“重启即
	// 误报卡住”，同时持续无进展超过阈值后仍能判出。
	for _, t := range e.state.Tasks {
		if !t.Running || !t.LastProgressAt.IsZero() {
			continue
		}
		if inc := e.state.Incidents[alertKey(t.Pipeline, t.Operator, CauseStuck)]; inc != nil && inc.State == StateFiring {
			continue
		}
		anchor := e.state.LastIngest[operatorKey(t.Pipeline, t.Operator)]
		if anchor.IsZero() {
			anchor = e.state.EffectiveNow
		}
		t.LastProgressAt = anchor
	}
	e.logf("engine opened: restored=%t buckets=%d tasks=%d incidents=%d",
		store != nil && e.state.SavedAt != time.Time{}, len(e.state.Buckets),
		len(e.state.Tasks), len(e.state.Incidents))
	return e, nil
}

// Close 刷盘并关闭引擎。
func (e *Engine) Close(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	e.closed = true
	return e.persistLocked(ctx)
}

// SetLogger 注入日志输出（可传 nil 关闭日志）。
func (e *Engine) SetLogger(l Logger) { e.log = l }

// Ingest 接入一条事件（并发安全）。返回去重/过期判定及本次产生的告警。
func (e *Engine) Ingest(ctx context.Context, ev Event) (IngestResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return IngestResult{}, errors.New("engine: closed")
	}
	if ev.ID == "" {
		return IngestResult{}, errors.New("engine: event ID required")
	}
	if ev.Pipeline == "" || ev.Operator == "" {
		return IngestResult{}, errors.New("engine: pipeline and operator required")
	}

	// 单调有效时间：处理时钟回拨时不后退，保证新鲜度等判定稳定可复现。
	now := e.advanceNowLocked()

	res, touched := e.classifyAndApplyLocked(ev, now)

	var fired, recovered []Alert
	if res.Verdict.Accepted {
		fired, recovered = e.evaluateAlerts(now, touched)
	}
	res.Alerts = fired
	res.Recovered = recovered

	if err := e.persistLocked(ctx); err != nil {
		return res, fmt.Errorf("engine: persist: %w", err)
	}
	for _, a := range fired {
		e.logf("ALERT FIRED key=%s cause=%s sev=%s fireCount=%d msg=%q",
			a.Key, a.Cause, a.Severity, a.FireCount, a.Message)
	}
	for _, a := range recovered {
		e.logf("ALERT RECOVERED key=%s cause=%s at=%s", a.Key, a.Cause, now.Format(time.RFC3339))
	}
	return res, nil
}

// classifyAndApplyLocked 完成去重、过期判定、统计并入（调用方持锁）。
func (e *Engine) classifyAndApplyLocked(ev Event, now time.Time) (IngestResult, map[string]bool) {
	s := e.state
	touched := map[string]bool{operatorKey(ev.Pipeline, ev.Operator): true}
	v := Verdict{EventID: ev.ID}

	// 1) 幂等去重：已正常处理或已标记过期的 ID 都不允许二次生效。
	//    持锁执行，并采用“插入即占座”：先检查，再立即写入，保证同 ID 的
	//    两个并发投递中恰好只有一个能进入后续统计。
	if _, dup := s.SeenEvents[ev.ID]; dup {
		v.Duplicate = true
		v.Reason = ReasonDuplicate
		v.Detail = "事件 ID 已处理过，重复投递不计数"
		e.logf("ingest duplicate id=%s", ev.ID)
		return IngestResult{Verdict: v}, touched
	}
	if _, wasExpired := s.ExpiredEvents[ev.ID]; wasExpired {
		v.Duplicate = true
		v.Reason = ReasonDuplicate
		v.Detail = "事件 ID 此前已标记为过期，重复投递保持过期结论"
		e.logf("ingest duplicate(previously-expired) id=%s", ev.ID)
		return IngestResult{Verdict: v}, touched
	}
	// 立即占座（若后续判定过期，会从 SeenEvents 移除并转入 ExpiredEvents）。
	s.SeenEvents[ev.ID] = now

	ev.OccurredAt = ev.OccurredAt.UTC()
	opK := operatorKey(ev.Pipeline, ev.Operator)

	// 2) 水位推进（事件时间语义）。水位单调不减：乱序、回拨都不会让它倒退。
	prevWM, hasWM := s.Watermarks[opK]
	newWM := prevWM
	if ev.Kind == KindWatermark {
		newWM = ev.Watermark.UTC()
	} else {
		if ev.OccurredAt.After(newWM) {
			newWM = ev.OccurredAt
		}
	}
	if hasWM && newWM.Before(prevWM) {
		newWM = prevWM
	}

	// 3) 迟到/过期判定（仅对携带事件时间的数据/任务事件，相对当前水位）。
	lateCutoff := prevWM.Add(-e.cfg.AllowedLateness)
	if hasWM && ev.Kind != KindWatermark && ev.OccurredAt.Before(lateCutoff) {
		// 超出容忍窗口：明确标记 expired 并留痕，不并入统计、不静默丢弃。
		delete(s.SeenEvents, ev.ID) // 撤销占座
		s.ExpiredEvents[ev.ID] = now
		v.Expired = true
		v.Accepted = false
		v.Reason = ReasonExpired
		v.WindowStart = windowFloor(ev.OccurredAt, e.cfg.WindowSize)
		v.Detail = fmt.Sprintf("事件时间 %s 早于容忍下界 %s（水位 %s - 容忍 %s），标记过期且不计数",
			ev.OccurredAt.Format(time.RFC3339Nano), lateCutoff.Format(time.RFC3339Nano),
			prevWM.Format(time.RFC3339Nano), e.cfg.AllowedLateness)
		// 过期事件不推进水位。
		e.logf("ingest EXPIRED id=%s occurred=%s cutoff=%s", ev.ID,
			ev.OccurredAt.Format(time.RFC3339Nano), lateCutoff.Format(time.RFC3339Nano))
		return IngestResult{Verdict: v}, touched
	}

	// 4) 正式并入：推进水位、更新统计（去重占座已在步骤 1 完成）。
	s.Watermarks[opK] = newWM
	s.LastIngest[opK] = now // 新鲜度以“有效接入”时间为准

	start := windowFloor(ev.OccurredAt, e.cfg.WindowSize)
	b := s.ensureBucket(ev.Pipeline, ev.Operator, start, e.cfg.WindowSize)
	v.WindowStart = start
	v.Accepted = true

	switch ev.Kind {
	case KindRecord:
		b.RecordCount++
		b.ByteCount += ev.Bytes
		latMS := now.Sub(ev.OccurredAt).Milliseconds()
		if latMS > 0 {
			b.LatencySumMS += latMS
		}
	case KindTaskStarted:
		b.TaskStarted++
		if _, exists := s.Tasks[ev.TaskID]; !exists {
			s.Tasks[ev.TaskID] = &taskState{
				TaskID: ev.TaskID, Pipeline: ev.Pipeline, Operator: ev.Operator,
				StartedAt: ev.OccurredAt, Running: true,
			}
		}
		// 任务已存在（例如 finished 先到、started 迟到，或同 ID 重投）时
		// 不覆盖既有状态，避免把已结束/失败的任务错误重置为运行中。
	case KindTaskFinished:
		b.TaskFinished++
		t := s.Tasks[ev.TaskID]
		if t == nil {
			// 未见到 started 也允许结束（迟到的 started 之后仍可关联），
			// 以 finished 事件时间补建，保证失败不丢。
			t = &taskState{TaskID: ev.TaskID, Pipeline: ev.Pipeline, Operator: ev.Operator,
				StartedAt: ev.OccurredAt, Running: true}
			s.Tasks[ev.TaskID] = t
		}
		t.Running = false
		t.FinishedAt = ev.OccurredAt
		t.Failed = ev.Failed
		t.ErrorMessage = ev.ErrorMessage
		// 结束本身是该任务的最后一次进展。
		t.LastProgressAt = now
		if ev.Failed {
			b.TaskFailed++
		}
	case KindWatermark:
		// 仅推进水位，无记录计数。
	}

	// 卡住判定的“进展”口径：同算子任何被接受的事件（数据记录、显式水位、
	// 该算子的任务事件）都算该算子名下仍在运行任务的一次进展；过期/重复
	// 事件在更早的分支已返回，不会走到这里，因此不会制造虚假进展。
	for _, t := range s.Tasks {
		if t.Running && t.Pipeline == ev.Pipeline && t.Operator == ev.Operator {
			t.LastProgressAt = now
		}
	}

	// 窗口内事件时间范围（迟到并入也如实反映）。
	if b.MinEventTime.IsZero() || ev.OccurredAt.Before(b.MinEventTime) {
		if ev.Kind != KindWatermark {
			b.MinEventTime = ev.OccurredAt
		}
	}
	if ev.OccurredAt.After(b.MaxEventTime) && ev.Kind != KindWatermark {
		b.MaxEventTime = ev.OccurredAt
	}
	b.UpdatedAt = now

	// 5) 水位越过末端的窗口标记关闭（迟到事件在容忍内仍可改写成 closed 桶）。
	for _, bk := range s.Buckets {
		if !bk.Closed && !newWM.Before(bk.WindowEnd) {
			bk.Closed = true
		}
	}

	within := "on-time"
	if hasWM && ev.OccurredAt.Before(prevWM) {
		within = "late-within-tolerance"
	}
	v.Reason = ReasonNone
	v.Detail = fmt.Sprintf("事件并入窗口 [%s,%s)，水位 %s->%s（%s）",
		start.Format(time.RFC3339), b.WindowEnd.Format(time.RFC3339),
		prevWM.Format(time.RFC3339Nano), newWM.Format(time.RFC3339Nano), within)
	e.logf("ingest accepted id=%s kind=%s occurred=%s window=%s %s", ev.ID, ev.Kind,
		ev.OccurredAt.Format(time.RFC3339Nano), start.Format(time.RFC3339), within)
	return IngestResult{Verdict: v}, touched
}

// persistLocked 将当前状态刷盘（store 为 nil 时仅内存运行）。
func (e *Engine) persistLocked(ctx context.Context) error {
	if e.store == nil {
		return nil
	}
	e.state.SavedAt = e.state.EffectiveNow
	e.state.Version = snapshotVersion
	return e.store.Save(ctx, e.state)
}

func (e *Engine) logf(format string, args ...any) {
	if e.log != nil {
		e.log.Printf(format, args...)
	}
}
