package main

import (
	"context"
	"fmt"
	"log"
	"time"

	GopherGraph "github.com/unclesam-LY/GopherGraph"
)

// AgentState 是这个示例工作流的状态
type AgentState struct {
	Input    string
	Draft    string
	Reviewed string
	Messages []string // 含引用类型，演示 StateCloner 的必要性
}

// deepClone 演示如何为含切片的状态做安全的深拷贝
func deepClone(s AgentState) AgentState {
	clone := s
	clone.Messages = append([]string{}, s.Messages...) // 切片深拷贝
	return clone
}

func main() {
	g := GopherGraph.NewGraph[AgentState]()

	// 节点 1：起草
	g.AddNode("drafter", func(ctx context.Context, s AgentState) (AgentState, error) {
		time.Sleep(80 * time.Millisecond) // 模拟耗时 AI 调用
		s.Draft = fmt.Sprintf("【草稿】%s 的初版内容", s.Input)
		s.Messages = append(s.Messages, "drafter: 完成起草")
		return s, nil
	})

	// 节点 2：审阅
	g.AddNode("reviewer", func(ctx context.Context, s AgentState) (AgentState, error) {
		time.Sleep(60 * time.Millisecond)
		s.Reviewed = s.Draft + "（已审阅 ✓）"
		s.Messages = append(s.Messages, "reviewer: 审阅完成")
		return s, nil
	})

	// 并发节点：同时翻译成英文和日文（演示 StateCloner）
	g.AddNode("translate_en", func(ctx context.Context, s AgentState) (AgentState, error) {
		time.Sleep(100 * time.Millisecond)
		s.Messages = append(s.Messages, fmt.Sprintf("translate_en: [EN] %s", s.Reviewed))
		return s, nil
	})
	g.AddNode("translate_ja", func(ctx context.Context, s AgentState) (AgentState, error) {
		time.Sleep(120 * time.Millisecond)
		s.Messages = append(s.Messages, fmt.Sprintf("translate_ja: [JA] %s", s.Reviewed))
		return s, nil
	})

	// 节点：发布
	g.AddNode("publisher", func(ctx context.Context, s AgentState) (AgentState, error) {
		s.Messages = append(s.Messages, "publisher: 🎉 发布成功")
		return s, nil
	})

	// 连线
	g.AddEdge("drafter", "reviewer")
	g.AddParallelEdges("reviewer", []string{"translate_en", "translate_ja"}, "publisher",
		func(ctx context.Context, parent AgentState, branches []AgentState) (AgentState, error) {
			// 合并所有分支的 Messages
			for _, b := range branches {
				parent.Messages = append(parent.Messages, b.Messages...)
			}
			parent.Messages = append(parent.Messages, "merger: 翻译分支已合并")
			return parent, nil
		},
	)

	cg, err := g.Compile()
	if err != nil {
		log.Fatalf("编译失败: %v", err)
	}

	// ============================================================
	// 使用 Engine 包装器，挂载所有增强能力
	// ============================================================
	var nodeTimings = make(map[string]time.Time)

	engine := GopherGraph.NewEngine(cg).
		// 1. 注册深拷贝函数，消除并发分支对 Messages 切片的数据竞争
		WithStateCloner(deepClone).
		// 2. 设置最大步数熔断（防御意外死循环）
		WithMaxSteps(50).
		// 3. Pre Hook：记录节点开始时间 + 向控制台推送"流式进度"
		WithPreNodeHook(func(ctx context.Context, name string, s AgentState) {
			nodeTimings[name] = time.Now()
			fmt.Printf("  ⏳ [PRE ] %-15s | 消息数: %d\n", name, len(s.Messages))
		}).
		// 4. Post Hook：计算并打印节点耗时
		WithPostNodeHook(func(ctx context.Context, name string, s AgentState) {
			elapsed := time.Since(nodeTimings[name])
			fmt.Printf("  ✅ [POST] %-15s | 耗时: %v | 消息数: %d\n", name, elapsed.Round(time.Millisecond), len(s.Messages))
		})

	fmt.Println("🚀 启动工作流...")
	fmt.Println("─────────────────────────────────────────────")

	totalStart := time.Now()
	thread, err := engine.Start(context.Background(), "drafter", AgentState{
		Input: "GopherGraph 技术文档",
	})
	if err != nil {
		log.Fatalf("运行失败: %v", err)
	}

	fmt.Println("─────────────────────────────────────────────")
	fmt.Printf("🏁 工作流结束，总耗时: %v\n\n", time.Since(totalStart).Round(time.Millisecond))
	fmt.Println("📋 最终消息日志：")
	for i, msg := range thread.State.Messages {
		fmt.Printf("   %d. %s\n", i+1, msg)
	}

	// ============================================================
	// 演示流式输出：通过 Context 注入 channel（不需要修改任何 NodeFn）
	// ============================================================
	fmt.Println("\n─────────────────────────────────────────────")
	fmt.Println("📡 演示：通过 Context 注入 channel 实现流式输出")
	fmt.Println("─────────────────────────────────────────────")

	type streamCtxKey struct{}
	streamCh := make(chan string, 10)

	// 在 Context 里注入 channel
	streamCtx := context.WithValue(context.Background(), streamCtxKey{}, streamCh)

	// 启动一个 goroutine 消费流式事件（模拟前端 SSE/WebSocket）
	go func() {
		for msg := range streamCh {
			fmt.Printf("  📨 [STREAM] %s\n", msg)
		}
	}()

	// Post Hook 里从 Context 取出 channel，写入流式事件
	streamEngine := GopherGraph.NewEngine(cg).
		WithStateCloner(deepClone).
		WithPostNodeHook(func(ctx context.Context, name string, s AgentState) {
			ch, ok := ctx.Value(streamCtxKey{}).(chan string)
			if ok {
				ch <- fmt.Sprintf("节点 [%s] 执行完毕，当前消息数: %d", name, len(s.Messages))
			}
		})

	_, err = streamEngine.Start(streamCtx, "drafter", AgentState{Input: "流式演示"})
	if err != nil {
		log.Fatalf("流式演示运行失败: %v", err)
	}
	close(streamCh)
	time.Sleep(50 * time.Millisecond) // 等待消费者 goroutine 打印完毕

	fmt.Println("\n✨ 示例结束")
}
