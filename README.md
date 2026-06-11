# GopherGraph

A lightweight, high-performance, and type-safe Multi-Agent workflow orchestration engine written in Go.  
基于 Go 语言（泛型）实现的轻量级、高性能、类型安全的多智能体（Multi-Agent）协同与任务编排引擎。  
Go 言語（ジェネリクス）で書かれた、軽量・高性能・型安全なマルチエージェント・ワークフロー協調エンジン。

---

* [简体中文](#-简体中文)
* [English](#-english)
* [日本語](#-日本語)

---

## 🇨🇳 简体中文

### 简介
`GopherGraph` 是一个用 Go 语言编写的代码即图（Code-as-Graph）智能体编排引擎，设计灵感来源于 Python 生态的 `LangGraph`。它利用 Go 1.18+ 的泛型机制，让工作流的上下文状态在编译期就具备强类型约束，并依靠 Go 原生的 Goroutines 和 Channels 实现极高并发的本地数据流转。它特别适合构建包含复杂循环（Looping）、并发节点执行（Parallel Execution）和人机协同（Human-in-the-Loop）的智能体工作流。

### 核心特性
- **强类型状态管理：** 利用 Go 泛型定义全局状态 `Graph[S]`，彻底告别松散的 `map[string]any` 和繁琐的类型断言。
- **支持循环结构：** 突破了传统 DAG（有向无环图）工作流的限制，原生支持节点间的双向循环和条件路由。
- **并发分支与短路取消：** 原生支持多个 Agent 节点的并发执行，基于 `context.WithCancel` 实现短路机制——任意分支出错立即取消其余兄弟 goroutine，避免算力空转。
- **并发安全深拷贝：** 通过 `Engine.WithStateCloner` 注入自定义深拷贝函数，彻底消除含引用类型的状态在并发分支中的数据竞争。
- **人机协同中断 (HITL)：** 支持在特定节点前自动挂起，返回执行线程快照，等待人工修改状态或审批通过后一键恢复 (`Resume`)。
- **死循环硬性熔断：** 通过 `Engine.WithMaxSteps` 设置步数上限，图中的任何意外环路都会被明确报错而非无限阻塞。
- **轻量级生命周期 Hooks：** `Engine.WithPreNodeHook` / `WithPostNodeHook` 提供节点执行前后的钩子，无需修改任何 `NodeFn`，即可接入日志、链路追踪（OpenTelemetry）或流式输出。
- **开箱即用持久化：** 提供 `Checkpointer[S]` 接口与内置的 `FileCheckpointer[S]`，支持将进度保存为 JSON 文件，实现停机或重启后进度无损恢复。
- **纯粹的高性能：** 基于内存流转，零外部依赖，极低的上下文切换开销。

### 快速开始

#### 1. 定义您的状态 (State)
```go
type MyState struct {
    Query    string
    Response string
    Value    int
    Messages []string // 含引用类型，需要配合 StateCloner 使用
}
```

#### 2. 直接使用 CompiledGraph（原始 API，向后兼容）
```go
package main

import (
    "context"
    GopherGraph "github.com/unclesam-LY/GopherGraph"
)

func main() {
    g := GopherGraph.NewGraph[MyState]()

    g.AddNode("start", startNode)
    g.AddNode("task1", task1Node)
    g.AddNode("task2", task2Node)
    g.AddNode("end", endNode)

    // 定义合并函数：将并发分支状态合并到主状态中
    merger := func(ctx context.Context, parent MyState, branches []MyState) (MyState, error) {
        for _, b := range branches {
            parent.Value += b.Value
        }
        return parent, nil
    }

    // 建立并发边：从 start 并发分流到 task1 和 task2，执行完后通过 merger 合并状态并去往 end 节点
    g.AddParallelEdges("start", []string{"task1", "task2"}, "end", merger)

    cg, _ := g.Compile()
    thread, _ := cg.Start(context.Background(), "start", MyState{})
}
```

#### 3. 使用 Engine 包装器（推荐：生产级增强）
```go
cg, _ := g.Compile()

engine := GopherGraph.NewEngine(cg).
    // 深拷贝：消除并发分支对 Messages 切片的数据竞争
    WithStateCloner(func(s MyState) MyState {
        clone := s
        clone.Messages = append([]string{}, s.Messages...)
        return clone
    }).
    // 步数熔断：防御图中意外形成的死循环
    WithMaxSteps(100).
    // Pre Hook：节点执行前触发（日志、链路追踪、流式推送等）
    WithPreNodeHook(func(ctx context.Context, name string, s MyState) {
        log.Printf("[PRE]  node=%-12s query=%q", name, s.Query)
    }).
    // Post Hook：节点执行后触发（指标上报、状态快照等）
    WithPostNodeHook(func(ctx context.Context, name string, s MyState) {
        log.Printf("[POST] node=%-12s value=%d", name, s.Value)
    })

thread, err := engine.Start(ctx, "start", MyState{Query: "你好"})
```

#### 4. 流式输出（通过 Context 注入 Channel）
不需要修改任何 `NodeFn` 签名，流式输出是节点的副作用，通过 Context 传递 channel 即可：
```go
type streamKey struct{}

streamCh := make(chan string, 10)
ctx := context.WithValue(context.Background(), streamKey{}, streamCh)

// 在 Post Hook 里从 Context 取出 channel，写入流式事件
engine := GopherGraph.NewEngine(cg).
    WithPostNodeHook(func(ctx context.Context, name string, s MyState) {
        if ch, ok := ctx.Value(streamKey{}).(chan string); ok {
            ch <- fmt.Sprintf("节点 [%s] 完成", name)
        }
    })

go func() {
    for msg := range streamCh { fmt.Println(msg) } // 前端消费
}()

engine.Start(ctx, "start", MyState{})
close(streamCh)
```

#### 5. 文件持久化 (Checkpointer) 示例
```go
// 创建一个文件存储器，指定存放状态文件的目录
fc, _ := GopherGraph.NewFileCheckpointer[MyState]("./checkpoints")

// 在遇到中断挂起时，将 thread 保存到磁盘
sessionID := "user-session-123"
fc.Save(context.Background(), sessionID, thread)

// 重启程序后，可以从磁盘重新加载进度并恢复执行
loadedThread, _ := fc.Load(context.Background(), sessionID)
thread, _ = cg.Resume(context.Background(), loadedThread, modifiedState)
```

#### 运行内置示例
```bash
# 完整的"AI翻译 -> 质量检测 -> 人工审核 -> 发布"交互式 Demo
go run examples/translation/main.go

# Engine 增强特性演示（Hooks、StateCloner、流式输出）
go run examples/hooks/main.go
```

#### 运行单元测试
```bash
# 含竞态检测器，确保并发分支无数据竞争
go test -v -race ./...
```

---

## 🇺🇸 English

### Introduction
`GopherGraph` is a Code-as-Graph agent orchestration engine built in Go, inspired by Python's `LangGraph`. By leveraging Go 1.18+ Generics, GopherGraph ensures that workflow states are strictly typed at compile-time. Powered by native Goroutines and Channels, it executes agent communication with microsecond latency, making it the perfect engine for building complex looping agent workflows with Human-in-the-Loop (HITL), concurrent branches, and state persistence requirements.

### Key Features
- **Strictly Typed State:** Bind your custom struct to `Graph[S]` via Go generics. Say goodbye to unsafe `map[string]any` and runtime type assertions.
- **Support for Cycles/Loops:** Unlike traditional DAG (Directed Acyclic Graph) engines, GopherGraph natively supports loops and dynamic routing based on agent evaluation.
- **Parallel Branching with Short-Circuit Cancellation:** Run multiple agent nodes concurrently. Built on `context.WithCancel`, a failure in any branch immediately cancels all sibling goroutines, preventing wasted compute on LLM API calls.
- **Deep-Clone for Concurrency Safety:** Inject a custom `StateCloner` via `Engine.WithStateCloner` to safely deep-copy reference types (slices, maps, pointers) before fan-out, eliminating data races at the root.
- **Human-in-the-Loop (HITL):** Pause workflow execution *before* a designated node, capture a snapshot (`Thread`), modify the state, and `Resume` seamlessly.
- **Infinite Loop Circuit Breaker:** Set a hard step limit with `Engine.WithMaxSteps`. Any accidental cycle in the graph returns a clear error instead of blocking forever.
- **Lightweight Lifecycle Hooks:** `Engine.WithPreNodeHook` / `WithPostNodeHook` fire before and after each node without touching any `NodeFn` signature — perfect for logging, OpenTelemetry tracing, or streaming output.
- **State Persistence (Checkpointer):** Generic `Checkpointer[S]` interface with built-in `FileCheckpointer[S]` for saving and loading execution snapshots to/from JSON files.
- **Zero-Dependency & High-Performance:** Written in pure Go with zero external dependencies, leveraging in-memory queues for lightning-fast orchestration.

### Quick Start

#### 1. Define Your State
```go
type MyState struct {
    Query    string
    Response string
    Value    int
    Messages []string // reference type — pair with StateCloner for concurrency safety
}
```

#### 2. Use CompiledGraph directly (original API, fully backwards-compatible)
```go
package main

import (
    "context"
    GopherGraph "github.com/unclesam-LY/GopherGraph"
)

func main() {
    g := GopherGraph.NewGraph[MyState]()

    g.AddNode("start", startNode)
    g.AddNode("task1", task1Node)
    g.AddNode("task2", task2Node)
    g.AddNode("end", endNode)

    // Define how to merge states from concurrent branches
    merger := func(ctx context.Context, parent MyState, branches []MyState) (MyState, error) {
        for _, b := range branches {
            parent.Value += b.Value
        }
        return parent, nil
    }

    // Branch from start to task1 and task2 in parallel, merge and transition to end
    g.AddParallelEdges("start", []string{"task1", "task2"}, "end", merger)

    cg, _ := g.Compile()
    thread, _ := cg.Start(context.Background(), "start", MyState{})
}
```

#### 3. Use the Engine wrapper (recommended for production)
```go
cg, _ := g.Compile()

engine := GopherGraph.NewEngine(cg).
    // Deep-copy state before fan-out to eliminate data races on reference types
    WithStateCloner(func(s MyState) MyState {
        clone := s
        clone.Messages = append([]string{}, s.Messages...)
        return clone
    }).
    // Hard step limit — prevents accidental infinite loops
    WithMaxSteps(100).
    // Pre-hook: fires before each node (logging, tracing, streaming notifications)
    WithPreNodeHook(func(ctx context.Context, name string, s MyState) {
        log.Printf("[PRE]  node=%-12s query=%q", name, s.Query)
    }).
    // Post-hook: fires after each node (metrics, snapshots)
    WithPostNodeHook(func(ctx context.Context, name string, s MyState) {
        log.Printf("[POST] node=%-12s value=%d", name, s.Value)
    })

thread, err := engine.Start(ctx, "start", MyState{Query: "hello"})
```

#### 4. Streaming output via Context-injected channel
No need to change any `NodeFn` signature. Streaming is a side effect — pass a channel through the Context:
```go
type streamKey struct{}

streamCh := make(chan string, 10)
ctx := context.WithValue(context.Background(), streamKey{}, streamCh)

engine := GopherGraph.NewEngine(cg).
    WithPostNodeHook(func(ctx context.Context, name string, s MyState) {
        if ch, ok := ctx.Value(streamKey{}).(chan string); ok {
            ch <- fmt.Sprintf("node [%s] done", name)
        }
    })

go func() {
    for msg := range streamCh { fmt.Println(msg) } // consume on the frontend
}()

engine.Start(ctx, "start", MyState{})
close(streamCh)
```

#### 5. State Persistence (Checkpointer) Example
```go
// Initialize a local directory storage
fc, _ := GopherGraph.NewFileCheckpointer[MyState]("./checkpoints")

// Save the thread snapshot when paused
sessionID := "user-session-123"
fc.Save(context.Background(), sessionID, thread)

// Reload the snapshot (e.g. after a process restart) and resume
loadedThread, _ := fc.Load(context.Background(), sessionID)
thread, _ = cg.Resume(context.Background(), loadedThread, modifiedState)
```

#### Run the Interactive Demo
```bash
# Interactive "Translation -> Review -> Human Approval -> Publish" demo
go run examples/translation/main.go

# Engine features demo (Hooks, StateCloner, Streaming)
go run examples/hooks/main.go
```

#### Run Unit Tests
```bash
# The -race flag verifies zero data races in parallel branches
go test -v -race ./...
```

---

## 🇯🇵 日本語

### 概要
`GopherGraph` は、Python エコシステムの `LangGraph` に着想を得て開発された、Go 言語向けの Code-as-Graph 型エージェントオーケストレーションエンジンです。Go 1.18+ のジェネリクスを活用することで、ワークフローの状態（State）をコンパイル時に厳密に型定義できます。Go 純正の Goroutines と Channels を利用したメモリ内メッセージングにより、高い並行性と極めて低いレイテンシを実現しています。自律的なループ（Looping）や、並行処理（Parallel Execution）、人間参加型（Human-in-the-Loop）の意思決定を伴うエージェントワークフローの開発に最適です。

### 主な特徴
- **型安全な状態管理:** ジェネリクスを用いて `Graph[S]` に構造体をバインドします。冗長な `map[string]any` やランタイム時の型アサーションから解放されます。
- **ループと条件付き分岐のサポート:** 従来の DAG（有向非巡回グラフ）の制限を超え、エージェントの判定に基づく条件付きルートや双方向のループをサポート。
- **並行処理とショートサーキットキャンセル:** `context.WithCancel` に基づく並行ブランチ実行を実装。いずれかのブランチでエラーが発生した瞬間に他の全 goroutine へキャンセルシグナルを伝播し、LLM API の無駄な呼び出しを防ぎます。
- **データ競合を防ぐ深いコピー（DeepClone）:** `Engine.WithStateCloner` でカスタムの深コピー関数を注入。参照型（スライス・マップ・ポインタ）を持つ状態でも、並行ブランチでのデータ競合を根本から解消します。
- **Human-in-the-Loop (HITL) の一時停止と再開:** 特定のノードの実行前に処理を自動停止し、スナップショット（`Thread`）を返します。承認や状態修正の後にシームレスに処理を再開（`Resume`）できます。
- **無限ループのサーキットブレーカー:** `Engine.WithMaxSteps` でステップ数の上限を設定。グラフ内に意図しないサイクルが発生しても、永久ブロックせず明確なエラーを返します。
- **軽量なライフサイクルフック:** `Engine.WithPreNodeHook` / `WithPostNodeHook` により、`NodeFn` のシグネチャを一切変更せずにロギング・トレーシング・ストリーミング出力を実装できます。
- **状態の永続化 (Checkpointer):** 抽象的な `Checkpointer[S]` インタフェースと、JSONファイルへの書き出し・読み込みを行う組み込みの `FileCheckpointer[S]` をサポート。サーバー再起動後の進捗復旧が可能。
- **ピュア Go & 高性能:** 外部依存関係ゼロ。メモリ内チャネルを用いた高速なコンテキスト切り替え。

### クイックスタート

#### 1. 状態（State）の定義
```go
type MyState struct {
    Query    string
    Response string
    Value    int
    Messages []string // 参照型 — 並行処理では StateCloner と組み合わせて使用
}
```

#### 2. CompiledGraph を直接使う（元の API、完全後方互換）
```go
package main

import (
    "context"
    GopherGraph "github.com/unclesam-LY/GopherGraph"
)

func main() {
    g := GopherGraph.NewGraph[MyState]()

    g.AddNode("start", startNode)
    g.AddNode("task1", task1Node)
    g.AddNode("task2", task2Node)
    g.AddNode("end", endNode)

    // 並行分岐したノードの状態をマージする関数を定義
    merger := func(ctx context.Context, parent MyState, branches []MyState) (MyState, error) {
        for _, b := range branches {
            parent.Value += b.Value
        }
        return parent, nil
    }

    // startからtask1とtask2を並行実行し、mergerでマージしてendノードに遷移
    g.AddParallelEdges("start", []string{"task1", "task2"}, "end", merger)

    cg, _ := g.Compile()
    thread, _ := cg.Start(context.Background(), "start", MyState{})
}
```

#### 3. Engine ラッパーを使う（本番環境向け・推奨）
```go
cg, _ := g.Compile()

engine := GopherGraph.NewEngine(cg).
    // ファンアウト前に参照型を深くコピーしてデータ競合を排除
    WithStateCloner(func(s MyState) MyState {
        clone := s
        clone.Messages = append([]string{}, s.Messages...)
        return clone
    }).
    // ステップ上限 — 意図しない無限ループを防止
    WithMaxSteps(100).
    // Pre フック: 各ノード実行前に発火（ロギング・トレーシング・ストリーミング通知）
    WithPreNodeHook(func(ctx context.Context, name string, s MyState) {
        log.Printf("[PRE]  node=%-12s query=%q", name, s.Query)
    }).
    // Post フック: 各ノード実行後に発火（メトリクス・スナップショット）
    WithPostNodeHook(func(ctx context.Context, name string, s MyState) {
        log.Printf("[POST] node=%-12s value=%d", name, s.Value)
    })

thread, err := engine.Start(ctx, "start", MyState{Query: "こんにちは"})
```

#### 4. Context 経由の Channel によるストリーミング出力
`NodeFn` のシグネチャを変更する必要はありません。ストリーミングは副作用として Context 経由で channel を渡すだけです：
```go
type streamKey struct{}

streamCh := make(chan string, 10)
ctx := context.WithValue(context.Background(), streamKey{}, streamCh)

engine := GopherGraph.NewEngine(cg).
    WithPostNodeHook(func(ctx context.Context, name string, s MyState) {
        if ch, ok := ctx.Value(streamKey{}).(chan string); ok {
            ch <- fmt.Sprintf("ノード [%s] 完了", name)
        }
    })

go func() {
    for msg := range streamCh { fmt.Println(msg) } // フロントエンド側で消費
}()

engine.Start(ctx, "start", MyState{})
close(streamCh)
```

#### 5. 状態の永続化（Checkpointer）のコード例
```go
// 保存先ディレクトリを指定してローカルファイルチェッカーを初期化
fc, _ := GopherGraph.NewFileCheckpointer[MyState]("./checkpoints")

// 一時停止時にスレッドスナップショットをファイルに保存
sessionID := "user-session-123"
fc.Save(context.Background(), sessionID, thread)

// スナップショットを読み込み、プロセス再起動後に処理を再開
loadedThread, _ := fc.Load(context.Background(), sessionID)
thread, _ = cg.Resume(context.Background(), loadedThread, modifiedState)
```

#### インタラクティブデモの実行
```bash
# 「AI翻訳 -> 監査 -> 人間による確認 -> 公開」のインタラクティブデモ
go run examples/translation/main.go

# Engine 機能デモ（Hooks・StateCloner・ストリーミング）
go run examples/hooks/main.go
```

#### ユニットテストの実行
```bash
# -race フラグで並行ブランチのデータ競合がゼロであることを検証
go test -v -race ./...
```
