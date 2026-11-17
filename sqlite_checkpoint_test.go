package GopherGraph

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type SQLiteTestState struct {
	Query string
	Count int
}

func TestSQLiteCheckpointerBasic(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_checkpoints.db")

	sc, err := NewSQLiteCheckpointer[SQLiteTestState](dbPath)
	if err != nil {
		t.Fatalf("Failed to create SQLiteCheckpointer: %v", err)
	}
	defer sc.Close()

	ctx := context.Background()
	threadID := "thread-1"

	// 验证未存入时 Load 返回错误
	_, err = sc.Load(ctx, threadID)
	if err == nil {
		t.Fatalf("Expected error when loading non-existent thread, got nil")
	}

	thread := &Thread[SQLiteTestState]{
		State:      SQLiteTestState{Query: "hello", Count: 1},
		NextNode:   "node1",
		IsPaused:   true,
		IsFinished: false,
	}

	// 存入快照
	if err := sc.Save(ctx, threadID, thread); err != nil {
		t.Fatalf("Failed to save thread: %v", err)
	}

	// 读取快照
	loaded, err := sc.Load(ctx, threadID)
	if err != nil {
		t.Fatalf("Failed to load thread: %v", err)
	}

	if loaded.State.Query != "hello" || loaded.State.Count != 1 {
		t.Errorf("Loaded state mismatch, got %+v", loaded.State)
	}
	if loaded.NextNode != "node1" || !loaded.IsPaused || loaded.IsFinished {
		t.Errorf("Loaded thread metadata mismatch, got %+v", loaded)
	}

	// 删除线程记录
	if err := sc.Delete(ctx, threadID); err != nil {
		t.Fatalf("Failed to delete thread: %v", err)
	}

	// 校验删除后 Load 失败
	_, err = sc.Load(ctx, threadID)
	if err == nil {
		t.Fatalf("Expected error after delete, got nil")
	}
}

func TestSQLiteCheckpointerMultiVersion(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "multiver.db")

	sc, err := NewSQLiteCheckpointer[SQLiteTestState](dbPath, WithSQLitePrettyJSON())
	if err != nil {
		t.Fatalf("Failed to create SQLiteCheckpointer: %v", err)
	}
	defer sc.Close()

	ctx := context.Background()
	threadID := "thread-multi"

	// 连续保存 3 个不同版本的线程状态
	t1 := &Thread[SQLiteTestState]{State: SQLiteTestState{Query: "step1", Count: 10}, NextNode: "node1"}
	t2 := &Thread[SQLiteTestState]{State: SQLiteTestState{Query: "step2", Count: 20}, NextNode: "node2"}
	t3 := &Thread[SQLiteTestState]{State: SQLiteTestState{Query: "step3", Count: 30}, NextNode: "node3", IsFinished: true}

	if err := sc.Save(ctx, threadID, t1); err != nil {
		t.Fatalf("Save v1 failed: %v", err)
	}
	if err := sc.Save(ctx, threadID, t2); err != nil {
		t.Fatalf("Save v2 failed: %v", err)
	}
	if err := sc.Save(ctx, threadID, t3); err != nil {
		t.Fatalf("Save v3 failed: %v", err)
	}

	// 1. 验证 ListVersions
	versions, err := sc.ListVersions(ctx, threadID)
	if err != nil {
		t.Fatalf("ListVersions failed: %v", err)
	}
	if len(versions) != 3 {
		t.Fatalf("Expected 3 versions, got %d", len(versions))
	}
	for i, v := range versions {
		expectedVer := int64(i + 1)
		if v.Version != expectedVer {
			t.Errorf("Version index %d expected version %d, got %d", i, expectedVer, v.Version)
		}
	}

	// 2. 验证 Load 默认取最新的 v3 (并且因为 IsFinished == true，包装了 ErrAlreadyFinished)
	latest, err := sc.Load(ctx, threadID)
	if !errors.Is(err, ErrAlreadyFinished) {
		t.Fatalf("Expected ErrAlreadyFinished when loading finished latest state, got: %v", err)
	}
	if latest.State.Query != "step3" || latest.State.Count != 30 {
		t.Errorf("Latest state mismatch, got %+v", latest.State)
	}

	// 3. 验证 LoadVersion 取历史版本 v1 和 v2
	v1State, err := sc.LoadVersion(ctx, threadID, 1)
	if err != nil {
		t.Fatalf("LoadVersion(1) failed: %v", err)
	}
	if v1State.State.Query != "step1" || v1State.State.Count != 10 || v1State.NextNode != "node1" {
		t.Errorf("v1 state mismatch, got %+v", v1State)
	}

	v2State, err := sc.LoadVersion(ctx, threadID, 2)
	if err != nil {
		t.Fatalf("LoadVersion(2) failed: %v", err)
	}
	if v2State.State.Query != "step2" || v2State.State.Count != 20 || v2State.NextNode != "node2" {
		t.Errorf("v2 state mismatch, got %+v", v2State)
	}

	// 4. 验证 LoadVersion(3) 也因为 IsFinished 报 ErrAlreadyFinished
	v3State, err := sc.LoadVersion(ctx, threadID, 3)
	if !errors.Is(err, ErrAlreadyFinished) {
		t.Fatalf("Expected ErrAlreadyFinished for LoadVersion(3), got: %v", err)
	}
	if v3State.State.Query != "step3" {
		t.Errorf("v3 state mismatch, got %+v", v3State)
	}
}

func TestSQLiteCheckpointerPruneVersions(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "prune.db")

	sc, err := NewSQLiteCheckpointer[SQLiteTestState](dbPath)
	if err != nil {
		t.Fatalf("Failed to create SQLiteCheckpointer: %v", err)
	}
	defer sc.Close()

	ctx := context.Background()
	threadID := "thread-prune"

	// 存入 5 个版本
	for i := 1; i <= 5; i++ {
		tState := &Thread[SQLiteTestState]{
			State:    SQLiteTestState{Count: i},
			NextNode: "node",
		}
		if err := sc.Save(ctx, threadID, tState); err != nil {
			t.Fatalf("Save v%d failed: %v", i, err)
		}
	}

	// 保留最新的 2 个版本 (v4, v5)
	if err := sc.PruneVersions(ctx, threadID, 2); err != nil {
		t.Fatalf("PruneVersions failed: %v", err)
	}

	versions, err := sc.ListVersions(ctx, threadID)
	if err != nil {
		t.Fatalf("ListVersions failed: %v", err)
	}
	if len(versions) != 2 {
		t.Fatalf("Expected 2 versions after prune, got %d", len(versions))
	}
	if versions[0].Version != 4 || versions[1].Version != 5 {
		t.Errorf("Expected remaining versions 4 and 5, got %d and %d", versions[0].Version, versions[1].Version)
	}

	// 验证旧版本 v1 已不可读取
	_, err = sc.LoadVersion(ctx, threadID, 1)
	if err == nil {
		t.Fatalf("Expected error loading pruned version 1, got nil")
	}

	// 验证 v4 和 v5 依然能正常读取
	v4, err := sc.LoadVersion(ctx, threadID, 4)
	if err != nil || v4.State.Count != 4 {
		t.Errorf("LoadVersion(4) failed or mismatch: %v, state: %+v", err, v4)
	}
}

func TestSQLiteCheckpointerCustomTableAndExternalDB(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "custom.db")

	rawDB, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("Failed to open sqlite raw DB: %v", err)
	}

	sc, err := NewSQLiteCheckpointerWithDB[SQLiteTestState](rawDB, WithSQLiteTableName("my_custom_checkpoints"))
	if err != nil {
		rawDB.Close()
		t.Fatalf("Failed to create SQLiteCheckpointerWithDB: %v", err)
	}

	ctx := context.Background()
	threadID := "thread-custom"

	thread := &Thread[SQLiteTestState]{State: SQLiteTestState{Query: "custom table"}}
	if err := sc.Save(ctx, threadID, thread); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	loaded, err := sc.Load(ctx, threadID)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if loaded.State.Query != "custom table" {
		t.Errorf("Loaded state mismatch, got %s", loaded.State.Query)
	}

	// 测试 Close 不会关闭外部传入的 rawDB (ownsDB == false)
	if err := sc.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if err := rawDB.Ping(); err != nil {
		t.Fatalf("External DB should remain open after sc.Close(): %v", err)
	}
	rawDB.Close()
}

func TestSQLiteCheckpointerThreadIDValidation(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "val.db")

	sc, err := NewSQLiteCheckpointer[SQLiteTestState](dbPath)
	if err != nil {
		t.Fatalf("Failed to create SQLiteCheckpointer: %v", err)
	}
	defer sc.Close()

	ctx := context.Background()
	invalidIDs := []string{"", "../malicious", "foo/bar", "foo\\bar"}
	dummyThread := &Thread[SQLiteTestState]{State: SQLiteTestState{Query: "test"}}

	for _, id := range invalidIDs {
		if err := sc.Save(ctx, id, dummyThread); err == nil {
			t.Errorf("Expected Save error for invalid threadID %q, got nil", id)
		}
		if _, err := sc.Load(ctx, id); err == nil {
			t.Errorf("Expected Load error for invalid threadID %q, got nil", id)
		}
		if err := sc.Delete(ctx, id); err == nil {
			t.Errorf("Expected Delete error for invalid threadID %q, got nil", id)
		}
		if _, err := sc.ListVersions(ctx, id); err == nil {
			t.Errorf("Expected ListVersions error for invalid threadID %q, got nil", id)
		}
	}
}

func TestSQLiteCheckpointerConcurrency(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "concurrent.db")

	sc, err := NewSQLiteCheckpointer[SQLiteTestState](dbPath)
	if err != nil {
		t.Fatalf("Failed to create SQLiteCheckpointer: %v", err)
	}
	defer sc.Close()

	ctx := context.Background()
	threadID := "thread-concurrent"

	var wg sync.WaitGroup
	numRoutines := 10
	savesPerRoutine := 5

	for i := range numRoutines {
		wg.Add(1)
		go func(routineID int) {
			defer wg.Done()
			for j := range savesPerRoutine {
				tState := &Thread[SQLiteTestState]{
					State:    SQLiteTestState{Count: routineID*100 + j},
					NextNode: "step",
				}
				if err := sc.Save(ctx, threadID, tState); err != nil {
					t.Errorf("Concurrent Save failed for routine %d step %d: %v", routineID, j, err)
				}
				time.Sleep(1 * time.Millisecond)
			}
		}(i)
	}

	wg.Wait()

	versions, err := sc.ListVersions(ctx, threadID)
	if err != nil {
		t.Fatalf("ListVersions failed after concurrency test: %v", err)
	}

	expectedTotal := numRoutines * savesPerRoutine
	if len(versions) != expectedTotal {
		t.Fatalf("Expected %d total versions, got %d", expectedTotal, len(versions))
	}

	// 验证版本号单调递增且不重复 (1 ~ expectedTotal)
	for i, v := range versions {
		expectedVer := int64(i + 1)
		if v.Version != expectedVer {
			t.Errorf("Version mismatch at index %d, expected %d, got %d", i, expectedVer, v.Version)
		}
	}
}
