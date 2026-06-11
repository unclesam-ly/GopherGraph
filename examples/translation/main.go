package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	GopherGraph "github.com/unclesam-LY/GopherGraph"
)

// TranslationState 代表我们工作流的共享状态（State）
type TranslationState struct {
	InputText      string // 原文（中文）
	TranslatedText string // 译文（日语）
	ReviewNotes    string // 质检员反馈意见
	Approved       bool   // 是否已获批准
}

// 翻译员 Agent (Node)
func translatorNode(ctx context.Context, state TranslationState) (TranslationState, error) {
	fmt.Printf("[翻译员] 正在翻译原文: %q...\n", state.InputText)

	// 模拟翻译：如果是被拒绝退回的（含有 ReviewNotes），我们进行修正
	if state.ReviewNotes != "" {
		fmt.Printf("[翻译员] 收到质检意见：%q，正在修正...\n", state.ReviewNotes)
		if strings.Contains(state.InputText, "你好") {
			state.TranslatedText = "こんにちは（修正版）"
		} else {
			state.TranslatedText = state.InputText + " (修正後の日本語訳)"
		}
	} else {
		// 初次翻译
		if strings.Contains(state.InputText, "你好") {
			state.TranslatedText = "こんにちは"
		} else {
			state.TranslatedText = state.InputText + " (日本語訳)"
		}
	}
	state.ReviewNotes = "" // 翻译完成，清空历史意见
	return state, nil
}

// 质检员 Agent (Node)
func reviewerNode(ctx context.Context, state TranslationState) (TranslationState, error) {
	fmt.Printf("[质检员] 正在审核译文: %q...\n", state.TranslatedText)

	// 模拟审核：如果译文是完美的 "こんにちは" 或者包含 "修正" 字样，则通过；否则退回
	if state.TranslatedText == "こんにちは" {
		state.Approved = true
		fmt.Println("[质检员] 审核通过！直接发布。")
	} else if strings.Contains(state.TranslatedText, "修正") {
		state.Approved = true
		fmt.Println("[质检员] 修正版审核通过！")
	} else {
		state.Approved = false
		state.ReviewNotes = "翻译过于生硬，请重新翻译并润色"
		fmt.Println("[质检员] 审核不通过，建议退回重写。")
	}
	return state, nil
}

// 人工审核节点（这个节点被标记为中断点，运行前会暂停）
func humanReviewNode(ctx context.Context, state TranslationState) (TranslationState, error) {
	// 这个节点只有在 Resume 恢复执行后才会运行
	fmt.Printf("[系统] 人工审核节点已被触发。当前审批状态 Approved = %t\n", state.Approved)
	return state, nil
}

// 发布员 Agent (Node)
func publisherNode(ctx context.Context, state TranslationState) (TranslationState, error) {
	fmt.Printf("🎉 [发布员] 恭喜！内容发布成功！\n最终译文: %s\n", state.TranslatedText)
	return state, nil
}

// 路由函数：决定质检后去哪里
func reviewRouter(ctx context.Context, state TranslationState) (string, error) {
	if state.Approved {
		return "publisher", nil // 审核通过，去发布
	}

	// 审核不通过时：如果原文包含敏感词“秘密”，必须人工审核
	if strings.Contains(state.InputText, "秘密") {
		fmt.Println("[路由逻辑] 检测到敏感内容，必须路由到 [human_review] 进行人工审批")
		return "human_review", nil
	}

	// 否则，普通翻译错误，直接退回给翻译员重新处理
	fmt.Println("[路由逻辑] 普通翻译错误，退回给 [translator] 重新处理")
	return "translator", nil
}

// 路由函数：决定人工审核完去哪里
func humanRouter(ctx context.Context, state TranslationState) (string, error) {
	if state.Approved {
		return "publisher", nil // 人工同意，去发布
	}
	return "translator", nil // 人工拒绝，回滚到翻译员重写
}

func main() {
	// 1. 创建图并注册节点
	g := GopherGraph.NewGraph[TranslationState]()
	g.AddNode("translator", translatorNode)
	g.AddNode("reviewer", reviewerNode)
	g.AddNode("human_review", humanReviewNode)
	g.AddNode("publisher", publisherNode)
	// 2. 建立连线
	g.AddEdge("translator", "reviewer")
	g.AddConditionalEdges("reviewer", reviewRouter)
	g.AddConditionalEdges("human_review", humanRouter)
	// 3. 标记 human_review 节点为中断点（进入该节点前暂停）
	g.AddInterrupt("human_review")
	// 4. 编译图
	cg, err := g.Compile()
	if err != nil {
		fmt.Printf("编译失败: %v\n", err)
		return
	}
	// 5. 启动图
	// 包含敏感词“秘密”，会触发人工审核中断：
	inputText := "这是一个秘密你好"
	fmt.Printf("🚀 启动工作流，输入原文: %q\n", inputText)
	ctx := context.Background()
	thread, err := cg.Start(ctx, "translator", TranslationState{InputText: inputText})
	if err != nil {
		fmt.Printf("运行失败: %v\n", err)
		return
	}
	// 6. 循环检查线程状态，处理中断
	reader := bufio.NewReader(os.Stdin)
	for {
		if thread.IsFinished {
			break
		}
		if thread.IsPaused {
			fmt.Println("\n==================================================")
			fmt.Printf("⚠️  [工作流挂起] 遇到中断节点: %s\n", thread.NextNode)
			fmt.Printf("👉 当前状态：\n   - 原文: %s\n   - 译文: %s\n", thread.State.InputText, thread.State.TranslatedText)
			fmt.Print("🤔 请审批：[y] 批准发布 / [n] 拒绝退回 / [或者直接输入你修改后的译文]: ")
			input, _ := reader.ReadString('\n')
			input = strings.TrimSpace(input)
			// 根据人工输入更新状态
			currentState := thread.State
			if strings.ToLower(input) == "y" {
				currentState.Approved = true
				fmt.Println("👍 人工审核：批准发布")
			} else if strings.ToLower(input) == "n" {
				currentState.Approved = false
				currentState.ReviewNotes = "人工审核拒绝：请重新润色"
				fmt.Println("👎 人工审核：拒绝，退回重写")
			} else {
				// 用户直接输入了修改后的译文
				currentState.TranslatedText = input
				currentState.Approved = true
				fmt.Printf("✍️  人工审核：修改译文为 %q 并批准发布\n", input)
			}
			fmt.Println("==================================================")
			// 7. 恢复执行，并将修改后的状态注入
			thread, err = cg.Resume(ctx, thread, currentState)
			if err != nil {
				fmt.Printf("恢复失败: %v\n", err)
				return
			}
		}
	}
	fmt.Println("\n🏁 工作流顺利结束！")
}
