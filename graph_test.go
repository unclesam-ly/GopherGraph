package GopherGraph

import (
	"context"
	"errors"
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
	fc, err := NewFileCheckpointer[TestState](tmpDir)
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
