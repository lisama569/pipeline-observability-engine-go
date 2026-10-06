package pipelineobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// snapshotVersion 是当前持久化格式版本。
//
//	v1：Silences map[告警键]silenceState（仅全局/精确键两种）。
//	v2：SilenceRules map[规则ID]silenceRule（按 pipeline/operator/cause/task
//	    范围生效），事件单新增 Muted，任务新增 LastProgressAt。
//	加载 v1 快照时自动迁移 Silences -> SilenceRules。
const snapshotVersion = 2

// Store 是状态持久化抽象。引擎在每次改变状态后调用 Save，
// 启动时调用 Load 恢复。实现必须保证重启后状态完整延续。
type Store interface {
	// Save 原子化地持久化全部状态。
	Save(ctx context.Context, s *snapshot) error
	// Load 读取最近一次持久化的状态；无历史数据时返回 (nil, nil)。
	Load(ctx context.Context) (*snapshot, error)
}

// FileStore 把状态以 JSON 快照形式写入单个本地文件。
//
// 持久化语义：
//   - 写入采用“临时文件 + fsync + 原子 rename”，崩溃时要么看到旧的完整
//     快照、要么看到新的完整快照，不会读到半截文件；
//   - 每次接入成功后整体覆写（模拟本地持久化，数据量为测试规模）；
//   - 不依赖任何外部服务或网络。
type FileStore struct {
	path string
}

// NewFileStore 在 path 处创建文件存储（父目录需存在或用空串表示内存）。
func NewFileStore(path string) *FileStore { return &FileStore{path: path} }

// Save 实现 Store。
func (f *FileStore) Save(_ context.Context, s *snapshot) error {
	if f.path == "" {
		return errors.New("filestore: empty path")
	}
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("filestore: marshal: %w", err)
	}
	dir := filepath.Dir(f.path)
	tmp, err := os.CreateTemp(dir, ".snap-*.tmp")
	if err != nil {
		return fmt.Errorf("filestore: create temp: %w", err)
	}
	tmpName := tmp.Name()
	// 无论成功与否都尝试清理临时文件。
	defer os.Remove(tmpName)

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("filestore: write temp: %w", err)
	}
	if err := tmp.Sync(); err != nil { // fsync，确保落盘
		tmp.Close()
		return fmt.Errorf("filestore: sync: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("filestore: close temp: %w", err)
	}
	if err := os.Rename(tmpName, f.path); err != nil { // 原子替换
		return fmt.Errorf("filestore: rename: %w", err)
	}
	return nil
}

// Load 实现 Store。文件不存在时返回 (nil, nil)，表示首次启动。
func (f *FileStore) Load(_ context.Context) (*snapshot, error) {
	data, err := os.ReadFile(f.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("filestore: read: %w", err)
	}
	var s snapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("filestore: unmarshal: %w", err)
	}
	if err := s.normalizeAfterLoad(); err != nil {
		return nil, err
	}
	return &s, nil
}
