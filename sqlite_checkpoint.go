package GopherGraph

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// CheckpointVersion 保存多版本检查点的元数据信息。
type CheckpointVersion struct {
	ID         int64     `json:"id"`
	ThreadID   string    `json:"thread_id"`
	Version    int64     `json:"version"`
	NextNode   string    `json:"next_node"`
	IsPaused   bool      `json:"is_paused"`
	IsFinished bool      `json:"is_finished"`
	CreatedAt  time.Time `json:"created_at"`
}

// SQLiteCheckpointer 实现了基于 SQLite 数据库的多版本追加写入（Append-Only Multi-Version）状态持久化。
type SQLiteCheckpointer[S any] struct {
	db        *sql.DB
	tableName string
	pretty    bool
	ownsDB    bool
	mu        sync.RWMutex
}

// SQLiteCheckpointerOption 是 SQLiteCheckpointer 的可选配置项。
type SQLiteCheckpointerOption func(*sqliteCheckpointerConfig)

type sqliteCheckpointerConfig struct {
	tableName string
	pretty    bool
}

// WithSQLiteTableName 指定状态持久化存储的表名（默认 "checkpoints"）。
func WithSQLiteTableName(tableName string) SQLiteCheckpointerOption {
	return func(c *sqliteCheckpointerConfig) {
		if tableName != "" {
			c.tableName = tableName
		}
	}
}

// WithSQLitePrettyJSON 开启 JSON 缩进格式存储（适合开发调试阶段）。
func WithSQLitePrettyJSON() SQLiteCheckpointerOption {
	return func(c *sqliteCheckpointerConfig) {
		c.pretty = true
	}
}

var validTableNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

func validateTableName(name string) error {
	if !validTableNameRegex.MatchString(name) {
		return fmt.Errorf("invalid table name %q: must contain only alphanumeric characters and underscores", name)
	}
	return nil
}

// NewSQLiteCheckpointer 通过 SQLite 数据库文件路径创建一个多版本状态检查点管理器。
// 会自动打开 SQLite 连接并开启 WAL 模式以保障高并发读写性能。
func NewSQLiteCheckpointer[S any](dbPath string, opts ...SQLiteCheckpointerOption) (*SQLiteCheckpointer[S], error) {
	if dbPath == "" {
		return nil, fmt.Errorf("dbPath must not be empty")
	}

	// 连接 SQLite，配置 WAL 模式与 busy_timeout
	dsn := fmt.Sprintf("%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)", dbPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite db: %w", err)
	}

	sc, err := NewSQLiteCheckpointerWithDB[S](db, opts...)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	sc.ownsDB = true
	return sc, nil
}

// NewSQLiteCheckpointerWithDB 使用已有的 *sql.DB 实例创建一个 SQLite 检查点管理器。
func NewSQLiteCheckpointerWithDB[S any](db *sql.DB, opts ...SQLiteCheckpointerOption) (*SQLiteCheckpointer[S], error) {
	if db == nil {
		return nil, fmt.Errorf("db must not be nil")
	}

	cfg := &sqliteCheckpointerConfig{
		tableName: "checkpoints",
	}
	for _, opt := range opts {
		opt(cfg)
	}

	if err := validateTableName(cfg.tableName); err != nil {
		return nil, err
	}

	sc := &SQLiteCheckpointer[S]{
		db:        db,
		tableName: cfg.tableName,
		pretty:    cfg.pretty,
		ownsDB:    false,
	}

	if err := sc.initTable(); err != nil {
		return nil, err
	}

	return sc, nil
}

func (sc *SQLiteCheckpointer[S]) initTable() error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			thread_id TEXT NOT NULL,
			version INTEGER NOT NULL,
			next_node TEXT NOT NULL,
			is_paused BOOLEAN NOT NULL DEFAULT 0,
			is_finished BOOLEAN NOT NULL DEFAULT 0,
			data TEXT NOT NULL,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);
		CREATE INDEX IF NOT EXISTS idx_%s_thread_ver ON %s(thread_id, version DESC);
	`, sc.tableName, sc.tableName, sc.tableName)

	_, err := sc.db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to initialize checkpoints table %q: %w", sc.tableName, err)
	}
	return nil
}

// Save 将当前 Thread 追加保存为一个新的版本号记录（Append-Only）。
func (sc *SQLiteCheckpointer[S]) Save(ctx context.Context, threadID string, thread *Thread[S]) error {
	if err := validateThreadID(threadID); err != nil {
		return err
	}
	if thread == nil {
		return fmt.Errorf("thread must not be nil")
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()

	var data []byte
	var err error
	if sc.pretty {
		data, err = json.MarshalIndent(thread, "", "  ")
	} else {
		data, err = json.Marshal(thread)
	}
	if err != nil {
		return fmt.Errorf("failed to marshal thread: %w", err)
	}

	tx, err := sc.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	// 获取当前 threadID 的最大版本号
	var maxVersion sql.NullInt64
	queryMax := fmt.Sprintf("SELECT MAX(version) FROM %s WHERE thread_id = ?", sc.tableName)
	if err := tx.QueryRowContext(ctx, queryMax, threadID).Scan(&maxVersion); err != nil {
		return fmt.Errorf("failed to query max version: %w", err)
	}

	newVersion := int64(1)
	if maxVersion.Valid {
		newVersion = maxVersion.Int64 + 1
	}

	// 追加插入新版本记录
	insertQuery := fmt.Sprintf(`
		INSERT INTO %s (thread_id, version, next_node, is_paused, is_finished, data, created_at)
		VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
	`, sc.tableName)

	_, err = tx.ExecContext(ctx, insertQuery, threadID, newVersion, thread.NextNode, thread.IsPaused, thread.IsFinished, string(data))
	if err != nil {
		return fmt.Errorf("failed to insert checkpoint version %d: %w", newVersion, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit checkpoint transaction: %w", err)
	}

	return nil
}

// Load 读取指定 threadID 的最新版本快照。
// 若最新线程状态为已完结 (IsFinished == true)，会包装 ErrAlreadyFinished 作为额外信息返回。
func (sc *SQLiteCheckpointer[S]) Load(ctx context.Context, threadID string) (*Thread[S], error) {
	if err := validateThreadID(threadID); err != nil {
		return nil, err
	}

	sc.mu.RLock()
	defer sc.mu.RUnlock()

	query := fmt.Sprintf("SELECT data FROM %s WHERE thread_id = ? ORDER BY version DESC LIMIT 1", sc.tableName)
	var dataStr string
	err := sc.db.QueryRowContext(ctx, query, threadID).Scan(&dataStr)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("checkpoint for thread %q not found", threadID)
		}
		return nil, fmt.Errorf("failed to load latest checkpoint: %w", err)
	}

	var thread Thread[S]
	if err := json.Unmarshal([]byte(dataStr), &thread); err != nil {
		return nil, fmt.Errorf("failed to unmarshal thread data: %w", err)
	}

	if thread.IsFinished {
		return &thread, fmt.Errorf("thread %q has already finished: %w", threadID, ErrAlreadyFinished)
	}

	return &thread, nil
}

// LoadVersion 读取指定 threadID 的特定历史版本号 (version) 的快照。
func (sc *SQLiteCheckpointer[S]) LoadVersion(ctx context.Context, threadID string, version int64) (*Thread[S], error) {
	if err := validateThreadID(threadID); err != nil {
		return nil, err
	}
	if version <= 0 {
		return nil, fmt.Errorf("version must be positive")
	}

	sc.mu.RLock()
	defer sc.mu.RUnlock()

	query := fmt.Sprintf("SELECT data FROM %s WHERE thread_id = ? AND version = ?", sc.tableName)
	var dataStr string
	err := sc.db.QueryRowContext(ctx, query, threadID, version).Scan(&dataStr)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("checkpoint for thread %q version %d not found", threadID, version)
		}
		return nil, fmt.Errorf("failed to load checkpoint version %d: %w", version, err)
	}

	var thread Thread[S]
	if err := json.Unmarshal([]byte(dataStr), &thread); err != nil {
		return nil, fmt.Errorf("failed to unmarshal thread data: %w", err)
	}

	if thread.IsFinished {
		return &thread, fmt.Errorf("thread %q (version %d) has already finished: %w", threadID, version, ErrAlreadyFinished)
	}

	return &thread, nil
}

// ListVersions 获取指定 threadID 的所有历史版本元数据列表（按 version 升序排列）。
func (sc *SQLiteCheckpointer[S]) ListVersions(ctx context.Context, threadID string) ([]CheckpointVersion, error) {
	if err := validateThreadID(threadID); err != nil {
		return nil, err
	}

	sc.mu.RLock()
	defer sc.mu.RUnlock()

	query := fmt.Sprintf("SELECT id, thread_id, version, next_node, is_paused, is_finished, created_at FROM %s WHERE thread_id = ? ORDER BY version ASC", sc.tableName)
	rows, err := sc.db.QueryContext(ctx, query, threadID)
	if err != nil {
		return nil, fmt.Errorf("failed to list versions: %w", err)
	}
	defer rows.Close()

	var versions []CheckpointVersion
	for rows.Next() {
		var v CheckpointVersion
		if err := rows.Scan(&v.ID, &v.ThreadID, &v.Version, &v.NextNode, &v.IsPaused, &v.IsFinished, &v.CreatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan checkpoint version: %w", err)
		}
		versions = append(versions, v)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return versions, nil
}

// PruneVersions 对指定 threadID 的历史检查点进行剪枝清理，仅保留最新的 keepN 个版本。
func (sc *SQLiteCheckpointer[S]) PruneVersions(ctx context.Context, threadID string, keepN int) error {
	if err := validateThreadID(threadID); err != nil {
		return err
	}
	if keepN <= 0 {
		return fmt.Errorf("keepN must be greater than 0")
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()

	query := fmt.Sprintf(`
		DELETE FROM %s 
		WHERE thread_id = ? 
		  AND id NOT IN (
			SELECT id FROM %s WHERE thread_id = ? ORDER BY version DESC LIMIT ?
		  )
	`, sc.tableName, sc.tableName)

	_, err := sc.db.ExecContext(ctx, query, threadID, threadID, keepN)
	if err != nil {
		return fmt.Errorf("failed to prune versions: %w", err)
	}
	return nil
}

// Delete 删除指定 threadID 对应的所有版本历史快照记录（幂等操作）。
func (sc *SQLiteCheckpointer[S]) Delete(ctx context.Context, threadID string) error {
	if err := validateThreadID(threadID); err != nil {
		return err
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()

	query := fmt.Sprintf("DELETE FROM %s WHERE thread_id = ?", sc.tableName)
	_, err := sc.db.ExecContext(ctx, query, threadID)
	if err != nil {
		return fmt.Errorf("failed to delete checkpoints for thread %q: %w", threadID, err)
	}
	return nil
}

// Close 关闭由 NewSQLiteCheckpointer 创建的底层 SQLite 数据库连接。
// 若数据库连接是由外部创建并传给 NewSQLiteCheckpointerWithDB 的，则调用 Close 为无操作（由外部管理）。
func (sc *SQLiteCheckpointer[S]) Close() error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if sc.ownsDB && sc.db != nil {
		err := sc.db.Close()
		sc.db = nil
		return err
	}
	return nil
}
