package GopherGraph

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

// TestState 是测试用的强类型状态
type TestState struct {
	Value int
	Log   []string
}

// TestSequentialExecution 测试最基础的线性工作流 (A -> B)
func TestSequentialExecution(t *testing.T) {
	g := NewGraph[TestState]()

	g.AddNode("A", func(ctx context.Context, s TestState) (TestState, error) {
		s.Value += 1
		s.Log = append(s.Log, "A")
		return s, nil
	})
	g.AddNode("B", func(ctx context.Context, s TestState) (TestState, error) {
		s.Value *= 2
		s.Log = append(s.Log, "B")
		return s, nil
	})

	g.AddEdge("A", "B")

	cg, err := g.Compile()
	if err != nil {
		t.Fatalf("编译图失败: %v", err)
	}

	thread, err := cg.Start(context.Background(), "A", TestState{Value: 5})
	if err != nil {
		t.Fatalf("启动图失败: %v", err)
	}

	// 验证最终状态
	if !thread.IsFinished {
		t.Errorf("期望工作流运行结束，实际未结束")
	}
	// 计算公式: (5 + 1) * 2 = 12
	if thread.State.Value != 12 {
		t.Errorf("期望 Value 为 12，实际为 %d", thread.State.Value)
	}
	if len(thread.State.Log) != 2 || thread.State.Log[0] != "A" || thread.State.Log[1] != "B" {
		t.Errorf("执行路径日志不匹配，实际为: %v", thread.State.Log)
	}
}

// TestConditionalRouting 测试路由条件分支跳转 (start -> even/odd)
func TestConditionalRouting(t *testing.T) {
	g := NewGraph[TestState]()

	g.AddNode("start", func(ctx context.Context, s TestState) (TestState, error) {
		s.Log = append(s.Log, "start")
		return s, nil
	})
	g.AddNode("even", func(ctx context.Context, s TestState) (TestState, error) {
		s.Log = append(s.Log, "even")
		return s, nil
	})
	g.AddNode("odd", func(ctx context.Context, s TestState) (TestState, error) {
		s.Log = append(s.Log, "odd")
		return s, nil
	})

	g.AddConditionalEdges("start", func(ctx context.Context, s TestState) (string, error) {
		if s.Value%2 == 0 {
			return "even", nil
		}
		return "odd", nil
	})

	cg, err := g.Compile()
	if err != nil {
		t.Fatalf("编译图失败: %v", err)
	}

	// 测试偶数分支
	thread1, err := cg.Start(context.Background(), "start", TestState{Value: 4})
	if err != nil {
		t.Fatalf("启动图失败: %v", err)
	}
	if thread1.State.Log[1] != "even" {
		t.Errorf("偶数测试路由错误，实际路径: %v", thread1.State.Log)
	}

	// 测试奇数分支
	thread2, err := cg.Start(context.Background(), "start", TestState{Value: 7})
	if err != nil {
		t.Fatalf("启动图失败: %v", err)
	}
	if thread2.State.Log[1] != "odd" {
		t.Errorf("奇数测试路由错误，实际路径: %v", thread2.State.Log)
	}
}

// TestInterruptAndResume 测试中断挂起与恢复 (A -> [Interrupt B] -> C)
func TestInterruptAndResume(t *testing.T) {
	g := NewGraph[TestState]()

	g.AddNode("A", func(ctx context.Context, s TestState) (TestState, error) {
		s.Value += 10
		return s, nil
	})
	g.AddNode("B", func(ctx context.Context, s TestState) (TestState, error) {
		s.Value += 20
		return s, nil
	})
	g.AddNode("C", func(ctx context.Context, s TestState) (TestState, error) {
		s.Value += 30
		return s, nil
	})

	g.AddEdge("A", "B")
	g.AddEdge("B", "C")

	// 标记在执行 B 之前进行中断
	g.AddInterrupt("B")

	cg, err := g.Compile()
	if err != nil {
		t.Fatalf("编译图失败: %v", err)
	}

	// 1. 启动图，应该在执行 B 之前停下来
	thread, err := cg.Start(context.Background(), "A", TestState{Value: 0})
	if err != nil {
		t.Fatalf("启动图失败: %v", err)
	}

	// 验证是否成功停在 B 之前
	if !thread.IsPaused {
		t.Errorf("期望工作流处于 Paused 状态")
	}
	if thread.NextNode != "B" {
		t.Errorf("期望下一个节点是 B，实际是 %q", thread.NextNode)
	}
	if thread.State.Value != 10 { // 只执行了 A: 0 + 10 = 10
		t.Errorf("期望 Value 为 10，实际为 %d", thread.State.Value)
	}

	// 2. 模拟人工介入：修改状态值为 100，并调用 Resume 恢复执行
	thread, err = cg.Resume(context.Background(), thread, TestState{Value: 100})
	if err != nil {
		t.Fatalf("恢复图失败: %v", err)
	}

	// 验证是否顺利走完剩余的 B 和 C
	if !thread.IsFinished {
		t.Errorf("期望工作流已结束")
	}
	// 计算公式: B 执行 (100 + 20 = 120) -> C 执行 (120 + 30 = 150)
	if thread.State.Value != 150 {
		t.Errorf("期望最终 Value 为 150，实际为 %d", thread.State.Value)
	}
}

// TestTimeoutCancellation 测试通过 Context 对长时间运行的节点进行超时退出控制
func TestTimeoutCancellation(t *testing.T) {
	g := NewGraph[TestState]()

	g.AddNode("A", func(ctx context.Context, s TestState) (TestState, error) {
		select {
		case <-time.After(100 * time.Millisecond):
			s.Value = 1
			return s, nil
		case <-ctx.Done():
			return s, ctx.Err()
		}
	})

	cg, err := g.Compile()
	if err != nil {
		t.Fatalf("编译图失败: %v", err)
	}

	// 创建一个 20 毫秒的超短超时 Context，而节点 A 需要 100 毫秒才能跑完
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err = cg.Start(ctx, "A", TestState{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("期望返回 context.DeadlineExceeded 错误，实际返回: %v", err)
	}
}

// TestParallelExecution 测试并发分流与合并逻辑 (start -> [task1, task2 并发] -> merger -> end)
func TestParallelExecution(t *testing.T) {
	g := NewGraph[TestState]()
	// 1. 注册节点
	g.AddNode("start", func(ctx context.Context, s TestState) (TestState, error) {
		s.Log = append(s.Log, "start")
		return s, nil
	})
	g.AddNode("task1", func(ctx context.Context, s TestState) (TestState, error) {
		s.Log = append(s.Log, "task1")
		s.Value += 10
		return s, nil
	})
	g.AddNode("task2", func(ctx context.Context, s TestState) (TestState, error) {
		s.Log = append(s.Log, "task2")
		s.Value += 20
		return s, nil
	})
	g.AddNode("end", func(ctx context.Context, s TestState) (TestState, error) {
		s.Log = append(s.Log, "end")
		return s, nil
	})
	// 定义合并函数：将分支产生的 Value 相加，合并日志
	merger := func(ctx context.Context, parent TestState, branches []TestState) (TestState, error) {
		parent.Log = append(parent.Log, "merged")
		for _, b := range branches {
			parent.Value += b.Value
			parent.Log = append(parent.Log, b.Log...)
		}
		return parent, nil
	}
	// 2. 建立并发连线：从 start 分流并发执行 task1 和 task2，执行完后通过 merger 合并状态并去往 end 节点
	g.AddParallelEdges("start", []string{"task1", "task2"}, "end", merger)
	cg, err := g.Compile()
	if err != nil {
		t.Fatalf("编译图失败: %v", err)
	}
	// 3. 运行工作流
	thread, err := cg.Start(context.Background(), "start", TestState{Value: 0})
	if err != nil {
		t.Fatalf("启动图失败: %v", err)
	}
	// 4. 验证结果
	if !thread.IsFinished {
		t.Errorf("期望工作流正常结束")
	}
	// 验证 Value 是否被正确累加: task1 (10) + task2 (20) = 30
	if thread.State.Value != 30 {
		t.Errorf("期望 Value 为 30，实际为 %d", thread.State.Value)
	}
	// 验证日志中是否同时包含两个并发节点的执行结果（且顺序不固定，但都必须存在）
	logStr := strings.Join(thread.State.Log, ",")
	expectedLogs := []string{"start", "merged", "task1", "task2", "end"}
	for _, exp := range expectedLogs {
		if !strings.Contains(logStr, exp) {
			t.Errorf("期望日志中包含 %q，实际日志为: %v", exp, thread.State.Log)
		}
	}
}

// TestFileCheckpointer 测试文件检查点管理器对工作流进度的持久化与恢复
func TestFileCheckpointer(t *testing.T) {
	// 创建测试临时文件夹（测试结束后 Go 会自动清理）
	tmpDir := t.TempDir()
	fc, err := NewFileCheckpointer[TestState](tmpDir, WithPrettyJSON())
	if err != nil {
		t.Fatalf("创建文件检查点管理器失败: %v", err)
	}

	g := NewGraph[TestState]()
	g.AddNode("A", func(ctx context.Context, s TestState) (TestState, error) {
		s.Value = 100
		return s, nil
	})
	g.AddNode("B", func(ctx context.Context, s TestState) (TestState, error) {
		s.Value += 200
		return s, nil
	})

	g.AddEdge("A", "B")
	g.AddInterrupt("B") // 暂停在 B 之前

	cg, err := g.Compile()
	if err != nil {
		t.Fatalf("编译失败: %v", err)
	}

	// 1. 运行并触发中断
	thread, err := cg.Start(context.Background(), "A", TestState{Value: 0})
	if err != nil {
		t.Fatalf("运行失败: %v", err)
	}
	if !thread.IsPaused || thread.State.Value != 100 {
		t.Fatalf("未成功触发中断，当前值: %d", thread.State.Value)
	}

	// 2. 模拟人工介入前：将线程快照（内存数据）写入磁盘
	sessionID := "test-session-123"
	ctx := context.Background()
	if err := fc.Save(ctx, sessionID, thread); err != nil {
		t.Fatalf("保存状态失败: %v", err)
	}

	// 3. 从磁盘重新读取状态（模拟程序崩掉重启后，重新从文件加载）
	loadedThread, err := fc.Load(ctx, sessionID)
	if err != nil {
		t.Fatalf("读取状态失败: %v", err)
	}

	// 校验反序列化出来的状态是否完好无损
	if loadedThread.NextNode != "B" || !loadedThread.IsPaused || loadedThread.State.Value != 100 {
		t.Fatalf("加载出的状态不符合预期: %+v", loadedThread)
	}

	// 4. 使用从磁盘加载出来的线程指针，传入新状态恢复执行
	thread, err = cg.Resume(ctx, loadedThread, TestState{Value: 500})
	if err != nil {
		t.Fatalf("恢复执行失败: %v", err)
	}

	// 验证最终执行结果: 500 + 200 = 700
	if !thread.IsFinished || thread.State.Value != 700 {
		t.Errorf("执行结果错误，最终值: %d, 是否结束: %t", thread.State.Value, thread.IsFinished)
	}

	// 5. 保存已完结的线程，Load 后应附带 ErrAlreadyFinished 提示
	if err := fc.Save(ctx, sessionID, thread); err != nil {
		t.Fatalf("保存已完结线程失败: %v", err)
	}
	_, loadErr := fc.Load(ctx, sessionID)
	if !errors.Is(loadErr, ErrAlreadyFinished) {
		t.Errorf("Load 已完结线程应附带 ErrAlreadyFinished，实际: %v", loadErr)
	}

	// 6. Delete 后文件应不存在
	if err := fc.Delete(ctx, sessionID); err != nil {
		t.Fatalf("删除检查点失败: %v", err)
	}
	_, err = fc.Load(ctx, sessionID)
	if err == nil {
		t.Error("删除后 Load 应报错（文件不存在）")
	}
	// 幂等：重复 Delete 不应报错
	if err := fc.Delete(ctx, sessionID); err != nil {
		t.Errorf("重复 Delete 应幂等不报错，实际: %v", err)
	}
}

// TestMaxStepsCircuitBreaker 验证 Engine.WithMaxSteps 能在步数超限时熔断，防止死循环
func TestMaxStepsCircuitBreaker(t *testing.T) {
	g := NewGraph[TestState]()

	// 构造一个永远在 A -> B -> A 之间循环的图
	g.AddNode("A", func(ctx context.Context, s TestState) (TestState, error) {
		s.Value++
		return s, nil
	})
	g.AddNode("B", func(ctx context.Context, s TestState) (TestState, error) {
		s.Value++
		return s, nil
	})

	// A -> B -> A：形成闭环
	g.AddEdge("A", "B")
	g.AddEdge("B", "A")

	cg, err := g.Compile()
	if err != nil {
		t.Fatalf("编译图失败: %v", err)
	}

	// 设置最大步数为 5，图会在第 5 步触发熔断
	engine := NewEngine(cg).WithMaxSteps(5)
	_, err = engine.Start(context.Background(), "A", TestState{})
	if err == nil {
		t.Fatal("期望返回步数超限错误，实际无错误返回")
	}
	if !strings.Contains(err.Error(), "max steps") {
		t.Errorf("期望错误信息包含 'max steps'，实际: %v", err)
	}
}

// TestConcurrentCancellation 验证并发分支中一个节点报错时，其他兄弟节点能被及时取消
func TestConcurrentCancellation(t *testing.T) {
	g := NewGraph[TestState]()

	g.AddNode("start", func(ctx context.Context, s TestState) (TestState, error) {
		return s, nil
	})
	// task_fail：立即返回错误
	g.AddNode("task_fail", func(ctx context.Context, s TestState) (TestState, error) {
		return s, errors.New("task_fail intentional error")
	})
	// task_long：模拟耗时操作，应感知取消并提前退出
	g.AddNode("task_long", func(ctx context.Context, s TestState) (TestState, error) {
		select {
		case <-time.After(5 * time.Second): // 5 秒，正常不应该跑完
			s.Value = 999
			return s, nil
		case <-ctx.Done():
			return s, ctx.Err()
		}
	})

	g.AddParallelEdges("start", []string{"task_fail", "task_long"}, "", func(_ context.Context, parent TestState, _ []TestState) (TestState, error) {
		return parent, nil
	})

	cg, err := g.Compile()
	if err != nil {
		t.Fatalf("编译图失败: %v", err)
	}

	start := time.Now()
	_, err = cg.Start(context.Background(), "start", TestState{})
	elapsed := time.Since(start)

	// 验证确实返回了错误
	if err == nil {
		t.Fatal("期望返回并发分支错误，实际无错误返回")
	}
	if !strings.Contains(err.Error(), "task_fail intentional error") {
		t.Errorf("期望错误来自 task_fail，实际: %v", err)
	}
	// 验证 task_long 被快速取消，整个并发组的耗时远小于 5 秒
	if elapsed > 2*time.Second {
		t.Errorf("期望并发组在取消后快速结束（< 2s），实际耗时: %v", elapsed)
	}
}

// TestLifecycleHooks 验证 Engine 的 Pre/Post 钩子在每个节点执行前后均被正确触发
func TestLifecycleHooks(t *testing.T) {
	g := NewGraph[TestState]()

	g.AddNode("node1", func(ctx context.Context, s TestState) (TestState, error) {
		s.Value += 1
		return s, nil
	})
	g.AddNode("node2", func(ctx context.Context, s TestState) (TestState, error) {
		s.Value += 10
		return s, nil
	})
	g.AddEdge("node1", "node2")

	cg, err := g.Compile()
	if err != nil {
		t.Fatalf("编译图失败: %v", err)
	}

	var hookLog []string

	engine := NewEngine(cg).
		WithPreNodeHook(func(ctx context.Context, name string, s TestState) {
			hookLog = append(hookLog, "pre:"+name)
		}).
		WithPostNodeHook(func(ctx context.Context, name string, s TestState) {
			hookLog = append(hookLog, "post:"+name)
		})

	thread, err := engine.Start(context.Background(), "node1", TestState{})
	if err != nil {
		t.Fatalf("启动图失败: %v", err)
	}
	if !thread.IsFinished {
		t.Error("期望工作流运行结束")
	}
	// 最终值：node1(+1) -> node2(+10) = 11
	if thread.State.Value != 11 {
		t.Errorf("期望 Value 为 11，实际为 %d", thread.State.Value)
	}

	// 验证 Hook 触发顺序：pre:node1 -> post:node1 -> pre:node2 -> post:node2
	expected := []string{"pre:node1", "post:node1", "pre:node2", "post:node2"}
	if len(hookLog) != len(expected) {
		t.Fatalf("期望 %d 次 hook 触发，实际 %d 次: %v", len(expected), len(hookLog), hookLog)
	}
	for i, entry := range expected {
		if hookLog[i] != entry {
			t.Errorf("第 %d 次 hook 期望 %q，实际 %q", i, entry, hookLog[i])
		}
	}
}

// TestResumeErrors 验证 Resume 对非暂停/已结束线程的错误路径
func TestResumeErrors(t *testing.T) {
	g := NewGraph[TestState]()
	g.AddNode("A", func(ctx context.Context, s TestState) (TestState, error) {
		return s, nil
	})
	cg, err := g.Compile()
	if err != nil {
		t.Fatalf("编译图失败: %v", err)
	}

	// 1. Resume 非暂停的线程
	thread := &Thread[TestState]{NextNode: "A"}
	_, err = cg.Resume(context.Background(), thread, TestState{})
	if !errors.Is(err, ErrNotPaused) {
		t.Errorf("期望 ErrNotPaused，实际: %v", err)
	}

	// 2. Resume 已结束的线程
	thread2 := &Thread[TestState]{IsPaused: true, IsFinished: true}
	_, err = cg.Resume(context.Background(), thread2, TestState{})
	if !errors.Is(err, ErrAlreadyFinished) {
		t.Errorf("期望 ErrAlreadyFinished，实际: %v", err)
	}

	// 3. Engine.Resume 也应返回相同的哨兵错误
	engine := NewEngine(cg)
	_, err = engine.Resume(context.Background(), &Thread[TestState]{NextNode: "A"}, TestState{})
	if !errors.Is(err, ErrNotPaused) {
		t.Errorf("Engine.Resume: 期望 ErrNotPaused，实际: %v", err)
	}
}

// TestCompileValidationErrors 验证 Compile 对各种非法图结构的校验
func TestCompileValidationErrors(t *testing.T) {
	// 1. 空图
	g1 := NewGraph[TestState]()
	_, err := g1.Compile()
	if err == nil || !strings.Contains(err.Error(), "no nodes") {
		t.Errorf("空图应报错: %v", err)
	}

	// 2. 边指向不存在的节点
	g2 := NewGraph[TestState]()
	g2.AddNode("A", func(ctx context.Context, s TestState) (TestState, error) { return s, nil })
	g2.AddEdge("A", "nonexistent")
	_, err = g2.Compile()
	if err == nil || !strings.Contains(err.Error(), "destination") {
		t.Errorf("边指向不存在节点应报 destination 错误: %v", err)
	}

	// 3. 中断节点不存在
	g3 := NewGraph[TestState]()
	g3.AddNode("A", func(ctx context.Context, s TestState) (TestState, error) { return s, nil })
	g3.AddInterrupt("ghost")
	_, err = g3.Compile()
	if err == nil || !strings.Contains(err.Error(), "interrupt") {
		t.Errorf("不存在的中断节点应报错: %v", err)
	}

	// 4. 重复注册节点和边
	g4 := NewGraph[TestState]()
	g4.AddNode("A", func(ctx context.Context, s TestState) (TestState, error) { return s, nil })
	g4.AddNode("A", func(ctx context.Context, s TestState) (TestState, error) { return s, nil }) // 重复节点
	g4.AddEdge("A", "B")
	g4.AddEdge("A", "C") // 重复静态边
	g4.AddConditionalEdges("A", func(ctx context.Context, s TestState) (string, error) { return "B", nil })
	g4.AddConditionalEdges("A", func(ctx context.Context, s TestState) (string, error) { return "C", nil }) // 重复条件边
	g4.AddParallelEdges("A", []string{"B"}, "", func(ctx context.Context, parent TestState, branches []TestState) (TestState, error) {
		return parent, nil
	})
	g4.AddParallelEdges("A", []string{"C"}, "", func(ctx context.Context, parent TestState, branches []TestState) (TestState, error) {
		return parent, nil
	}) // 重复并发边

	_, err = g4.Compile()
	if err == nil {
		t.Fatal("重复注册应报错")
	}
	errMsg := err.Error()
	if !strings.Contains(errMsg, "already registered") {
		t.Errorf("应包含节点重复错误: %v", err)
	}
	if !strings.Contains(errMsg, "static edge registered") {
		t.Errorf("应包含静态边重复错误: %v", err)
	}
	if !strings.Contains(errMsg, "conditional edge registered") {
		t.Errorf("应包含条件边重复错误: %v", err)
	}
	if !strings.Contains(errMsg, "parallel edges registered") {
		t.Errorf("应包含并发边重复错误: %v", err)
	}
}

// TestCompileEdgeConflict 验证同一节点不能同时存在于多种出边类型中
func TestCompileEdgeConflict(t *testing.T) {
	g := NewGraph[TestState]()
	g.AddNode("A", func(ctx context.Context, s TestState) (TestState, error) { return s, nil })
	g.AddNode("B", func(ctx context.Context, s TestState) (TestState, error) { return s, nil })

	// 同时为 A 设置静态边和条件路由
	g.AddEdge("A", "B")
	g.AddConditionalEdges("A", func(ctx context.Context, s TestState) (string, error) {
		return "B", nil
	})

	_, err := g.Compile()
	if err == nil || !strings.Contains(err.Error(), "conflicting") {
		t.Errorf("出边冲突应报错: %v", err)
	}
}

// TestRouterReturnsNonExistentNode 验证路由函数返回不存在的节点名时的运行时错误
func TestRouterReturnsNonExistentNode(t *testing.T) {
	g := NewGraph[TestState]()
	g.AddNode("start", func(ctx context.Context, s TestState) (TestState, error) { return s, nil })
	g.AddConditionalEdges("start", func(ctx context.Context, s TestState) (string, error) {
		return "nonexistent_node", nil
	})

	cg, err := g.Compile()
	if err != nil {
		t.Fatalf("编译图失败: %v", err)
	}

	_, err = cg.Start(context.Background(), "start", TestState{})
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Errorf("路由到不存在节点应返回 runtime error: %v", err)
	}
}

// TestParallelWithStateCloner 验证 StateCloner 在并发分支中正确隔离引用类型
func TestParallelWithStateCloner(t *testing.T) {
	g := NewGraph[TestState]()

	g.AddNode("start", func(ctx context.Context, s TestState) (TestState, error) {
		s.Log = append(s.Log, "start")
		return s, nil
	})
	// 两个分支都向 Log 切片追加元素
	g.AddNode("branch1", func(ctx context.Context, s TestState) (TestState, error) {
		s.Log = append(s.Log, "branch1")
		s.Value = 10
		return s, nil
	})
	g.AddNode("branch2", func(ctx context.Context, s TestState) (TestState, error) {
		s.Log = append(s.Log, "branch2")
		s.Value = 20
		return s, nil
	})

	g.AddParallelEdges("start", []string{"branch1", "branch2"}, "",
		func(ctx context.Context, parent TestState, branches []TestState) (TestState, error) {
			for _, b := range branches {
				parent.Value += b.Value
			}
			return parent, nil
		},
	)

	cg, err := g.Compile()
	if err != nil {
		t.Fatalf("编译图失败: %v", err)
	}

	// 使用 Engine + StateCloner 运行
	engine := NewEngine(cg).WithStateCloner(func(s TestState) TestState {
		clone := s
		clone.Log = append([]string{}, s.Log...)
		return clone
	})

	thread, err := engine.Start(context.Background(), "start", TestState{})
	if err != nil {
		t.Fatalf("启动图失败: %v", err)
	}
	if thread.State.Value != 30 {
		t.Errorf("期望 Value 为 30，实际为 %d", thread.State.Value)
	}
}

// TestHookChaining 验证多次调用 WithPreNodeHook/WithPostNodeHook 时钩子按顺序叠加
func TestHookChaining(t *testing.T) {
	g := NewGraph[TestState]()
	g.AddNode("A", func(ctx context.Context, s TestState) (TestState, error) {
		return s, nil
	})
	cg, _ := g.Compile()

	var log []string
	engine := NewEngine(cg).
		WithPreNodeHook(func(ctx context.Context, name string, s TestState) {
			log = append(log, "pre1:"+name)
		}).
		WithPreNodeHook(func(ctx context.Context, name string, s TestState) {
			log = append(log, "pre2:"+name)
		}).
		WithPostNodeHook(func(ctx context.Context, name string, s TestState) {
			log = append(log, "post1:"+name)
		}).
		WithPostNodeHook(func(ctx context.Context, name string, s TestState) {
			log = append(log, "post2:"+name)
		})

	engine.Start(context.Background(), "A", TestState{})

	expected := []string{"pre1:A", "pre2:A", "post1:A", "post2:A"}
	if len(log) != len(expected) {
		t.Fatalf("期望 %d 次 hook 触发，实际 %d 次: %v", len(expected), len(log), log)
	}
	for i, entry := range expected {
		if log[i] != entry {
			t.Errorf("第 %d 次 hook 期望 %q，实际 %q", i, entry, log[i])
		}
	}
}

// TestCheckpointerThreadIDValidation 验证 FileCheckpointer 对恶意 threadID 的防御
func TestCheckpointerThreadIDValidation(t *testing.T) {
	tmpDir := t.TempDir()
	fc, err := NewFileCheckpointer[TestState](tmpDir)
	if err != nil {
		t.Fatalf("创建 FileCheckpointer 失败: %v", err)
	}

	thread := &Thread[TestState]{State: TestState{Value: 1}}
	ctx := context.Background()

	// 空 ID
	if err := fc.Save(ctx, "", thread); err == nil {
		t.Error("空 threadID 应报错")
	}

	// 路径穿越
	if err := fc.Save(ctx, "../escape", thread); err == nil {
		t.Error("包含 .. 的 threadID 应报错")
	}

	// 目录分隔符
	if err := fc.Save(ctx, "sub/dir", thread); err == nil {
		t.Error("包含 / 的 threadID 应报错")
	}

	// Load 同样校验
	_, err = fc.Load(ctx, "../escape")
	if err == nil {
		t.Error("Load 包含 .. 的 threadID 应报错")
	}

	// 合法 ID 应成功
	if err := fc.Save(ctx, "valid-id_123", thread); err != nil {
		t.Errorf("合法 threadID 不应报错: %v", err)
	}

	// Delete 校验
	if err := fc.Delete(ctx, "../escape"); err == nil {
		t.Error("Delete 包含 .. 的 threadID 应报错")
	}
}

// TestFileCheckpointerCompactJSON 验证默认格式为紧凑 JSON（不带缩进）
func TestFileCheckpointerCompactJSON(t *testing.T) {
	tmpDir := t.TempDir()

	// 默认紧凑格式
	fc, _ := NewFileCheckpointer[TestState](tmpDir)
	thread := &Thread[TestState]{State: TestState{Value: 42}, NextNode: "A"}
	_ = fc.Save(context.Background(), "compact", thread)

	data, _ := os.ReadFile(tmpDir + "/compact.json")
	if len(data) == 0 {
		t.Fatal("文件为空")
	}
	// 紧凑格式不应包含换行符
	if strings.Contains(string(data), "\n") {
		t.Error("默认模式应为紧凑 JSON，不应包含换行符")
	}

	// WithPrettyJSON 模式应包含换行符
	fcPretty, _ := NewFileCheckpointer[TestState](tmpDir, WithPrettyJSON())
	_ = fcPretty.Save(context.Background(), "pretty", thread)
	dataPretty, _ := os.ReadFile(tmpDir + "/pretty.json")
	if !strings.Contains(string(dataPretty), "\n") {
		t.Error("WithPrettyJSON 模式应包含换行符")
	}
}

// TestParallelExternalContextCancel 验证外部主动取消父 Context 时，并发分支不会吞掉 context.Canceled
func TestParallelExternalContextCancel(t *testing.T) {
	g := NewGraph[TestState]()
	g.AddNode("start", func(ctx context.Context, s TestState) (TestState, error) { return s, nil })
	g.AddNode("b1", func(ctx context.Context, s TestState) (TestState, error) {
		<-ctx.Done()
		return s, ctx.Err()
	})
	g.AddNode("b2", func(ctx context.Context, s TestState) (TestState, error) {
		<-ctx.Done()
		return s, ctx.Err()
	})
	g.AddParallelEdges("start", []string{"b1", "b2"}, "", func(ctx context.Context, p TestState, b []TestState) (TestState, error) {
		return p, nil
	})

	cg, err := g.Compile()
	if err != nil {
		t.Fatalf("编译失败: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	_, err = cg.Start(ctx, "start", TestState{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("期望返回 context.Canceled，实际返回: %v", err)
	}
}

// TestStartNodeInterruptAndValidation 验证首节点中断与非法 startNode 校验
func TestStartNodeInterruptAndValidation(t *testing.T) {
	g := NewGraph[TestState]()
	g.AddNode("A", func(ctx context.Context, s TestState) (TestState, error) {
		s.Value += 10
		return s, nil
	})
	g.AddInterrupt("A")

	cg, err := g.Compile()
	if err != nil {
		t.Fatalf("编译失败: %v", err)
	}

	// 1. 空 startNode 应报错
	if _, err := cg.Start(context.Background(), "", TestState{}); err == nil {
		t.Error("空 startNode 应返回错误")
	}

	// 2. 不存在的 startNode 应报错
	if _, err := cg.Start(context.Background(), "ghost", TestState{}); err == nil {
		t.Error("不存在的 startNode 应返回错误")
	}

	// 3. 首节点被标记为 Interrupt 时，Start 应在执行 A 之前暂停
	thread, err := cg.Start(context.Background(), "A", TestState{Value: 1})
	if err != nil {
		t.Fatalf("Start 失败: %v", err)
	}
	if !thread.IsPaused || thread.NextNode != "A" || thread.State.Value != 1 {
		t.Fatalf("期望在 A 执行前暂停且 Value=1，实际: %+v", thread)
	}

	// 4. Resume 后应正常执行 A 并结束
	thread, err = cg.Resume(context.Background(), thread, thread.State)
	if err != nil {
		t.Fatalf("Resume 失败: %v", err)
	}
	if !thread.IsFinished || thread.State.Value != 11 {
		t.Fatalf("期望 Resume 后完成且 Value=11，实际: %+v", thread)
	}
}

// TestConditionalEdgesWithTargetsAndMermaid 验证带目标声明的条件边校验与 Mermaid 流程图导出
func TestConditionalEdgesWithTargetsAndMermaid(t *testing.T) {
	noop := func(ctx context.Context, s TestState) (TestState, error) { return s, nil }

	// 1. 编译期校验：声明了不存在的目标节点应报错
	gInvalid := NewGraph[TestState]()
	gInvalid.AddNode("start", noop)
	gInvalid.AddConditionalEdgesWithTargets("start", []string{"ghost"}, func(ctx context.Context, s TestState) (string, error) {
		return "ghost", nil
	})
	if _, err := gInvalid.Compile(); err == nil || !strings.Contains(err.Error(), "conditional edge target") {
		t.Fatalf("期望编译期报错 conditional edge target，实际: %v", err)
	}

	// 2. 运行时校验：router 返回了未在 targets 中声明的节点应报错
	gUndeclared := NewGraph[TestState]()
	gUndeclared.AddNode("start", noop)
	gUndeclared.AddNode("A", noop)
	gUndeclared.AddNode("B", noop)
	gUndeclared.AddConditionalEdgesWithTargets("start", []string{"A"}, func(ctx context.Context, s TestState) (string, error) {
		return "B", nil // B 存在，但未在 targets 列表中声明
	})
	cgUndeclared, err := gUndeclared.Compile()
	if err != nil {
		t.Fatalf("编译失败: %v", err)
	}
	if _, err := cgUndeclared.Start(context.Background(), "start", TestState{}); err == nil || !strings.Contains(err.Error(), "undeclared target") {
		t.Fatalf("期望运行时报 undeclared target 错误，实际: %v", err)
	}

	// 3. Mermaid 导出完整性验证（含静态边、条件边、并发边、中断节点、保留字节点 "end"）
	g := NewGraph[TestState]()
	g.AddNode("start", noop)
	g.AddNode("reviewer", noop)
	g.AddNode("human_review", noop)
	g.AddNode("task1", noop)
	g.AddNode("task2", noop)
	g.AddNode("end", noop)

	g.AddEdge("start", "reviewer")
	g.AddConditionalEdgesWithTargets("reviewer", []string{"human_review", "task1", ""}, func(ctx context.Context, s TestState) (string, error) {
		return "task1", nil
	})
	g.AddParallelEdges("human_review", []string{"task1", "task2"}, "end", func(ctx context.Context, p TestState, b []TestState) (TestState, error) {
		return p, nil
	})
	g.AddInterrupt("human_review")

	cg, err := g.Compile()
	if err != nil {
		t.Fatalf("编译失败: %v", err)
	}

	mermaid := NewEngine(cg).Mermaid()
	expectedSnippets := []string{
		"flowchart TD",
		`human_review(["⏸ human_review (Interrupt)"])`,
		`node_end["end"]`, // 保留字 end 自动转义为 node_end
		"start --> reviewer",
		"reviewer -.->|conditional| human_review",
		"reviewer -.->|conditional| task1",
		`reviewer -.->|conditional| __END__(["🏁 END"])`,
		"human_review ==>|parallel| task1",
		"task1 ==>|merge| node_end",
		"style human_review",
	}
	for _, snippet := range expectedSnippets {
		if !strings.Contains(mermaid, snippet) {
			t.Errorf("Mermaid 输出缺少期望片段 %q\n完整输出:\n%s", snippet, mermaid)
		}
	}
}

// TestRetryPolicyAndErrorHook 验证节点自动重试（含 RetryIf 过滤）与 ErrorHook 触发闭环
func TestRetryPolicyAndErrorHook(t *testing.T) {
	var attempts int
	var errHookCalled int
	var lastHookErr error

	g := NewGraph[TestState]()
	// flaky 节点：前 2 次失败，第 3 次成功，且每次向切片写入脏数据（测试重试隔离）
	g.AddNode("flaky", func(ctx context.Context, s TestState) (TestState, error) {
		attempts++
		s.Log = append(s.Log, "attempt")
		if attempts < 3 {
			return s, errors.New("temporary 429 rate limit")
		}
		s.Value = 42
		return s, nil
	})
	// fatal 节点：返回不可重试的错误
	g.AddNode("fatal", func(ctx context.Context, s TestState) (TestState, error) {
		return s, errors.New("permanent auth error")
	})
	g.AddEdge("flaky", "fatal")

	cg, err := g.Compile()
	if err != nil {
		t.Fatalf("编译失败: %v", err)
	}

	engine := NewEngine(cg).
		WithStateCloner(func(s TestState) TestState {
			clone := s
			clone.Log = append([]string(nil), s.Log...)
			return clone
		}).
		WithRetryPolicy(RetryPolicy{
			MaxAttempts:    3,
			InitialBackoff: 5 * time.Millisecond,
			Multiplier:     2.0,
			RetryIf: func(err error) bool {
				return strings.Contains(err.Error(), "temporary")
			},
		}).
		WithErrorHook(func(ctx context.Context, name string, s TestState, err error) {
			errHookCalled++
			lastHookErr = err
		})

	thread, err := engine.Start(context.Background(), "flaky", TestState{})
	if err == nil || !strings.Contains(err.Error(), "permanent auth error") {
		t.Fatalf("期望在 fatal 节点报错退出，实际: %v", err)
	}

	// flaky 应该重试了 3 次后成功，且 StateCloner 保证了前两次失败的 append 不会污染第 3 次成功的状态
	if attempts != 3 {
		t.Errorf("期望 flaky 尝试 3 次，实际 %d 次", attempts)
	}
	if thread.State.Value != 42 || len(thread.State.Log) != 1 {
		t.Errorf("期望 flaky 成功后 Value=42 且 Log 长度为 1，实际: %+v", thread.State)
	}

	// ErrorHook 只应在 fatal 节点最终失败时触发 1 次（flaky 重试成功后不应触发 ErrorHook）
	if errHookCalled != 1 || lastHookErr == nil || !strings.Contains(lastHookErr.Error(), "permanent auth error") {
		t.Errorf("期望 ErrorHook 触发 1 次且记录 fatal 错误，实际次数=%d, err=%v", errHookCalled, lastHookErr)
	}
}
