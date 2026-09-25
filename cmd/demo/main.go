// Command demo 在本地用文件快照完整演示流水线可观测性引擎：
// 并发接入、乱序/迟到/重复/过期事件、数据滞后、任务超时/失败、
// 告警去重/静默/恢复以及进程重启后的状态延续。
//
// 运行：
//
//	go run ./cmd/demo            # 首次运行，产生快照 demo-state.json
//	go run ./cmd/demo            # 再次运行即模拟“重启恢复”
//	rm -f demo-state.json        # 回到全新状态
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	pipelineobs "github.com/lisama569/pipeline-observability-engine-go"
)

type stdLogger struct{}

func (stdLogger) Printf(format string, args ...any) {
	log.Printf("  "+format, args...)
}

func main() {
	ctx := context.Background()
	snapPath := filepath.Join(".", "demo-state.json")

	cfg := pipelineobs.Config{
		WindowSize:         time.Minute,
		AllowedLateness:    20 * time.Second,
		StalenessThreshold: 30 * time.Second,
		TaskTimeout:        90 * time.Second,
		SuppressWindow:     time.Minute,
	}

	clk := pipelineobs.NewMockClock(time.Date(2026, 9, 25, 10, 0, 0, 0, time.UTC))
	store := pipelineobs.NewFileStore(snapPath)
	restored := false
	if data, err := os.ReadFile(snapPath); err == nil && len(data) > 0 {
		restored = true
	}

	eng, err := pipelineobs.Open(ctx, cfg, clk, store)
	if err != nil {
		log.Fatal(err)
	}
	defer eng.Close(ctx)
	eng.SetLogger(stdLogger{})

	base := time.Date(2026, 9, 25, 10, 0, 0, 0, time.UTC)
	if !restored {
		log.Println("=== 全新启动：并发/乱序/重复/过期 演示 ===")

		// 1) 多 goroutine 并发、乱序接入同一窗口的记录事件（含 1 条重复）。
		var wg sync.WaitGroup
		events := []pipelineobs.Event{
			{ID: "r1", Pipeline: "p1", Operator: "source", Kind: pipelineobs.KindRecord, OccurredAt: base.Add(5 * time.Second), Bytes: 10},
			{ID: "r2", Pipeline: "p1", Operator: "source", Kind: pipelineobs.KindRecord, OccurredAt: base.Add(15 * time.Second), Bytes: 20},
			{ID: "r3", Pipeline: "p1", Operator: "source", Kind: pipelineobs.KindRecord, OccurredAt: base.Add(10 * time.Second), Bytes: 30}, // 乱序
			{ID: "r1", Pipeline: "p1", Operator: "source", Kind: pipelineobs.KindRecord, OccurredAt: base.Add(5 * time.Second), Bytes: 10},  // 重复
		}
		for _, ev := range events {
			wg.Add(1)
			go func(ev pipelineobs.Event) {
				defer wg.Done()
				r, err := eng.Ingest(ctx, ev)
				if err != nil {
					log.Fatal(err)
				}
				fmt.Printf("[接入 %s] accepted=%t duplicate=%t expired=%t reason=%s\n",
					ev.ID, r.Verdict.Accepted, r.Verdict.Duplicate, r.Verdict.Expired, r.Verdict.Reason)
			}(ev)
		}
		wg.Wait()

		// 2) 水位推进到 10:01:10 后：容忍内迟到（10:00:55，并入）与
		//    超窗过期（10:00:30，早于 10:01:10-20s=10:00:50）。
		clk.Set(base.Add(70 * time.Second))
		ingest := func(ev pipelineobs.Event) {
			r, err := eng.Ingest(ctx, ev)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("[接入 %s] accepted=%t expired=%t reason=%s :: %s\n",
				ev.ID, r.Verdict.Accepted, r.Verdict.Expired, r.Verdict.Reason, r.Verdict.Detail)
		}
		ingest(pipelineobs.Event{ID: "adv1", Pipeline: "p1", Operator: "source", Kind: pipelineobs.KindRecord, OccurredAt: base.Add(70 * time.Second)})
		ingest(pipelineobs.Event{ID: "late1", Pipeline: "p1", Operator: "source", Kind: pipelineobs.KindRecord, OccurredAt: base.Add(55 * time.Second), Bytes: 5})
		ingest(pipelineobs.Event{ID: "old1", Pipeline: "p1", Operator: "source", Kind: pipelineobs.KindRecord, OccurredAt: base.Add(30 * time.Second), Bytes: 99})

		// 3) 任务超时：任务 10:00 开始，水位推进超过 90s 仍未结束。
		ingest(pipelineobs.Event{ID: "t1-start", Pipeline: "p1", Operator: "agg", Kind: pipelineobs.KindTaskStarted, TaskID: "task-1", OccurredAt: base})
		clk.Set(base.Add(100 * time.Second))
		ingest(pipelineobs.Event{ID: "adv2", Pipeline: "p1", Operator: "agg", Kind: pipelineobs.KindRecord, OccurredAt: base.Add(100 * time.Second)})

		// 4) 任务失败（不同算子）。
		ingest(pipelineobs.Event{ID: "t2-start", Pipeline: "p1", Operator: "sink", Kind: pipelineobs.KindTaskStarted, TaskID: "task-2", OccurredAt: base.Add(100 * time.Second)})
		ingest(pipelineobs.Event{ID: "t2-fail", Pipeline: "p1", Operator: "sink", Kind: pipelineobs.KindTaskFinished, TaskID: "task-2", OccurredAt: base.Add(105 * time.Second), Failed: true, ErrorMessage: "connection reset"})

		// 5) 数据滞后：推进时钟但不接入 source 的事件，再 ObserveTime。
		clk.Set(base.Add(140 * time.Second))
		if _, err := eng.ObserveTime(ctx); err != nil {
			log.Fatal(err)
		}

		// 6) 时钟回拨：结论保持稳定（不重复告警）。
		log.Println("--- 时钟回拨 60s ---")
		clk.Set(base.Add(80 * time.Second))
		if _, err := eng.ObserveTime(ctx); err != nil {
			log.Fatal(err)
		}

		// 7) 任务结束 -> 超时告警恢复。
		clk.Set(base.Add(150 * time.Second))
		ingest(pipelineobs.Event{ID: "t1-fin", Pipeline: "p1", Operator: "agg", Kind: pipelineobs.KindTaskFinished, TaskID: "task-1", OccurredAt: base.Add(145 * time.Second)})

		log.Printf("快照已写入 %s，再次运行本程序可验证重启恢复（告警不刷屏、不丢失）。\n", snapPath)
	} else {
		log.Println("=== 重启恢复：未恢复告警与静默状态延续 ===")
		active := eng.ActiveAlerts()
		fmt.Printf("重启后当前 firing 告警数=%d\n", len(active))
		for _, a := range active {
			fmt.Printf("  firing: key=%s cause=%s fireCount=%d first=%s\n",
				a.Key, a.Cause, a.FireCount, a.FirstFiredAt.Format(time.RFC3339))
		}
		// 重放历史事件：全部判为重复，统计与告警不发生变化。
		r, err := eng.Ingest(ctx, pipelineobs.Event{ID: "r1", Pipeline: "p1", Operator: "source", Kind: pipelineobs.KindRecord, OccurredAt: base.Add(5 * time.Second)})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("[重启后重放 r1] duplicate=%t reason=%s；重启后 firing 告警数=%d（不刷屏）\n",
			r.Verdict.Duplicate, r.Verdict.Reason, len(eng.ActiveAlerts()))
	}

	fmt.Println("\n=== 聚合窗口 ===")
	for _, b := range eng.Buckets() {
		fmt.Printf("pipe=%s op=%-6s window=[%s,%s) closed=%t records=%d bytes=%d tasks(start/finish/fail)=%d/%d/%d\n",
			b.Pipeline, b.Operator, b.WindowStart.Format("15:04:05"), b.WindowEnd.Format("15:04:05"),
			b.Closed, b.RecordCount, b.ByteCount, b.TaskStarted, b.TaskFinished, b.TaskFailed)
	}
}
