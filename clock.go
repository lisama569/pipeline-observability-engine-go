// Package pipelineobs 提供进程内流水线可观测性引擎。
//
// clock.go 定义时钟抽象。所有时间判定都经过该抽象，便于测试注入
// 确定性时钟（包括时钟回拨场景）。
package pipelineobs

import "time"

// Clock 是可注入的时钟抽象。
type Clock interface {
	// Now 返回当前（处理）时间。
	Now() time.Time
}

// SystemClock 使用本机墙上时钟。
type SystemClock struct{}

// Now 返回当前系统时间（UTC）。
func (SystemClock) Now() time.Time { return time.Now().UTC() }

// MockClock 是手动可控时钟，用于确定性测试。
// 它可以直接 Set 到任意时刻（包括更早的时刻，模拟时钟回拨）。
type MockClock struct {
	t time.Time
}

// NewMockClock 以给定时刻创建可控时钟。
func NewMockClock(t time.Time) *MockClock { return &MockClock{t: t.UTC()} }

// Now 返回当前设定时刻。
func (c *MockClock) Now() time.Time { return c.t }

// Set 直接设定时刻（允许回拨）。
func (c *MockClock) Set(t time.Time) { c.t = t.UTC() }

// Advance 向前推进 d；d 为负则向后回拨。
func (c *MockClock) Advance(d time.Duration) { c.t = c.t.Add(d) }
