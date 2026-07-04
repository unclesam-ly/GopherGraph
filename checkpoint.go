package GopherGraph

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Checkpointer 定义了持久化状态的接口，S 是工作流的强类型状态。
type Checkpointer[S any] interface {
	// Save 保存当前线程快照。
	Save(ctx context.Context, threadID string, thread *Thread[S]) error
	// Load 读取之前保存的线程快照，用于恢复执行。
	Load(ctx context.Context, threadID string) (*Thread[S], error)
	// Delete 删除指定线程的快照文件，用于工作流完结后的清理。
	Delete(ctx context.Context, threadID string) error
}

// FileCheckpointer 实现了基于本地 JSON 文件的状态持久化。
type FileCheckpointer[S any] struct {
	dir    string // 存放状态文件的文件夹路径
	pretty bool   // 是否使用缩进格式（方便人工阅读，但体积更大）
}

// FileCheckpointerOption 是 FileCheckpointer 的可选配置项。
type FileCheckpointerOption func(*fileCheckpointerConfig)

type fileCheckpointerConfig struct {
	pretty bool
}

// WithPrettyJSON 开启 JSON 缩进格式（方便人工阅读，适合开发调试阶段）。
// 默认关闭，使用紧凑格式以减少磁盘 I/O 和文件体积。
func WithPrettyJSON() FileCheckpointerOption {
	return func(c *fileCheckpointerConfig) {
		c.pretty = true
	}
}

// NewFileCheckpointer 创建一个文件检查点管理器。
// 支持通过 opts 传入可选配置（如 WithPrettyJSON()）。
func NewFileCheckpointer[S any](dir string, opts ...FileCheckpointerOption) (*FileCheckpointer[S], error) {
	// 确保文件夹存在，不存在则创建
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create checkpoint dir: %w", err)
	}

	cfg := &fileCheckpointerConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	return &FileCheckpointer[S]{dir: dir, pretty: cfg.pretty}, nil
}

// Save 将 Thread 序列化为 JSON 并写入本地文件。
// 采用"先写临时文件再原子重命名"的方式，防止写入中途程序崩溃导致文件损坏。
// 若存在上次崩溃残留的临时文件，会在写入前自动清理。
func (fc *FileCheckpointer[S]) Save(ctx context.Context, threadID string, thread *Thread[S]) error {
	if err := validateThreadID(threadID); err != nil {
		return err
	}

	path := filepath.Join(fc.dir, threadID+".json")
	tmpPath := path + ".tmp"

	// 序列化：生产环境默认使用紧凑格式以减少磁盘 I/O；开发阶段可用 WithPrettyJSON() 开启缩进
	var data []byte
	var err error
	if fc.pretty {
		data, err = json.MarshalIndent(thread, "", "  ")
	} else {
		data, err = json.Marshal(thread)
	}
	if err != nil {
		return fmt.Errorf("failed to marshal thread: %w", err)
	}

	// 清理上次崩溃可能残留的临时文件（忽略不存在的错误）
	_ = os.Remove(tmpPath)

	// 原子写入：先写临时文件，再 Rename 替换，确保断电/崩溃安全
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write temp file: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		// Rename 失败时，尝试清理临时文件，避免残留
		_ = os.Remove(tmpPath)
		return fmt.Errorf("failed to rename temp file: %w", err)
	}
	return nil
}

// Load 从本地文件中读取 JSON 并反序列化为 Thread。
// 若加载到一个已完结（IsFinished == true）的线程，会包装 ErrAlreadyFinished 作为额外信息返回，
// 方便调用方做幂等处理（注意：此时 thread 依然有效，err 可通过 errors.Is 判断）。
func (fc *FileCheckpointer[S]) Load(ctx context.Context, threadID string) (*Thread[S], error) {
	if err := validateThreadID(threadID); err != nil {
		return nil, err
	}

	path := filepath.Join(fc.dir, threadID+".json")

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read thread file: %w", err)
	}

	var thread Thread[S]
	if err := json.Unmarshal(data, &thread); err != nil {
		return nil, fmt.Errorf("failed to unmarshal thread: %w", err)
	}

	// 如果加载到已完结的线程，附带哨兵错误提示，方便调用方感知
	if thread.IsFinished {
		return &thread, fmt.Errorf("thread %q has already finished: %w", threadID, ErrAlreadyFinished)
	}

	return &thread, nil
}

// Delete 删除指定 threadID 对应的快照文件。
// 通常在工作流完结后调用，防止快照文件无限积累占用磁盘空间。
// 若文件不存在，静默返回 nil（幂等操作）。
func (fc *FileCheckpointer[S]) Delete(ctx context.Context, threadID string) error {
	if err := validateThreadID(threadID); err != nil {
		return err
	}

	path := filepath.Join(fc.dir, threadID+".json")
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete checkpoint file: %w", err)
	}
	return nil
}

// validateThreadID 校验 threadID 的合法性，防止路径穿越（path traversal）攻击。
// threadID 中不允许出现目录分隔符或 ".." 序列。
func validateThreadID(id string) error {
	if id == "" {
		return fmt.Errorf("threadID must not be empty")
	}
	if strings.ContainsAny(id, "/\\") || strings.Contains(id, "..") {
		return fmt.Errorf("threadID %q contains invalid characters", id)
	}
	return nil
}
