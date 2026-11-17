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

> **Q: GopherGraph 好用吗？**
> **A: 就像你问你女朋友爱不爱你一样——至少编译器不会欺骗你。**

`GopherGraph` 是一个用 Go 语言编写的代码即图（Code-as-Graph）智能体编排引擎，设计灵感来源于 Python 生态的 `LangGraph`。它利用 Go 1.18+ 的泛型机制，让工作流的上下文状态在编译期就具备强类型约束，并依靠 Go 原生的 Goroutines 和 Channels 实现极高并发的本地数据流转。它特别适合构建包含复杂循环（Looping）、并发节点执行（Parallel Execution）和人机协同（Human-in-the-Loop）的智能体工作流。


### 核心特性
- **强类型状态管理：** 利用 Go 泛型定义全局状态 `Graph[S]`，彻底告别松散的 `map[string]any` 和繁琐的类型断言。
- **支持循环结构：** 突破了传统 DAG（有向无环图）工作流的限制，原生支持节点间的双向循环和条件路由。
- **并发分支与短路取消：** 原生支持多个 Agent 节点的并发执行，基于 `context.WithCancel` 实现短路机制——任意分支出错立即取消其余兄弟 goroutine，避免算力空转。
- **并发安全深拷贝：** 通过 `Engine.WithStateCloner` 注入自定义深拷贝函数，彻底消除含引用类型的状态在并发分支中的数据竞争。
- **人机协同中断 (HITL)：** 支持在特定节点（含起始节点）执行前自动挂起，返回执行线程快照，等待人工修改状态或审批通过后一键恢复 (`Resume`)。支持 `ErrNotPaused`、`ErrAlreadyFinished` 哨兵错误，便于精准判断异常。
- **死循环硬性熔断：** 通过 `Engine.WithMaxSteps` 设置步数上限，图中的任何意外环路都会被明确报错而非无限阻塞。
- **轻量级生命周期 Hooks（支持多重链式累加）：** `Engine.WithPreNodeHook` / `WithPostNodeHook` / `WithErrorHook` 支持多次调用链式累加（不覆盖已有钩子），完整覆盖顺序节点与并发分支节点的成功/失败闭环，无需修改 `NodeFn`，极易解耦日志、链路追踪（OpenTelemetry）、错误告警或流式输出等逻辑。
- **节点自动重试（指数退避）：** 支持通过 `Engine.WithRetryPolicy`（全局）与 `Engine.WithNodeRetryPolicy`（单节点）配置最大尝试次数、指数退避间隔及 `RetryIf` 错误过滤函数；自动跳过 `context.Canceled` / `context.DeadlineExceeded`，配合 `StateCloner` 保证每次重试状态隔离。
- **编译期与运行时结构校验：** 图在编译 (`Compile`) 时会验证节点/边存在性、拦截并发目标节点上的中断配置，并严格检测同一节点的出边类型冲突；使用 `AddConditionalEdgesWithTargets` 声明可达目标后，还可在编译期静态校验目标存在性并在运行时拦截越界跳转。
- **Mermaid 流程图导出：** 通过 `CompiledGraph.Mermaid()` 或 `Engine.Mermaid()` 一键导出确定性（Deterministic）的 `flowchart TD` 流程图，清晰区分普通节点、中断节点、条件分支与并发分流/合并边。
- **显式可控的状态持久化：** 提供 `Checkpointer[S]` 接口，内置基于 JSON 文件的 `FileCheckpointer[S]`（原子重命名防损坏）以及基于纯 Go SQLite 的 `SQLiteCheckpointer[S]`（支持多版本追加写入 Append-Only、历史版本回溯与剪枝），并对 `threadID` 进行路径穿越防御性校验；同时提供 `Engine.WithSnapshotSaver` 回调，每步完成后返回包含 `Step`、`Node`、`Duration`（节点执行耗时）和完整 `Thread` 的 `StepSnapshot[S]`（支持 `ThreadJSON()` 一键序列化），方便直接存入用户的 MySQL / PostgreSQL / Redis 等自定义数据库，不引入隐式 I/O 开销。
- **纯 Go 高性能与无 CGO：** 引擎核心基于内存与 Goroutine/Channel 流转，内置持久化均采用纯 Go 实现（零 CGO 依赖），具备极低的上下文切换开销。

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
    GopherGraph "github.com/unclesam-ly/GopherGraph"
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
    // 深拷贝：消除并发分支与节点重试时对 Messages 切片的数据竞争/状态污染
    WithStateCloner(func(s MyState) MyState {
        clone := s
        clone.Messages = append([]string{}, s.Messages...)
        return clone
    }).
    // 步数熔断：防御图中意外形成的死循环
    WithMaxSteps(100).
    // 自动重试：为外部 API 调用等瞬态错误配置指数退避重试
    WithRetryPolicy(GopherGraph.RetryPolicy{
        MaxAttempts:    3,
        InitialBackoff: 100 * time.Millisecond,
        MaxBackoff:     2 * time.Second,
        Multiplier:     2.0,
    }).
    // 也可为特定节点单独覆写重试策略（优先级高于全局策略）
    WithNodeRetryPolicy("llm_call", GopherGraph.RetryPolicy{
        MaxAttempts:    5,
        InitialBackoff: 200 * time.Millisecond,
    }).
    // Pre Hook：节点执行前触发（日志、链路追踪、流式推送等）
    WithPreNodeHook(func(ctx context.Context, name string, s MyState) {
        log.Printf("[PRE]  node=%-12s query=%q", name, s.Query)
    }).
    // Post Hook：节点执行后触发（指标上报、状态快照等）
    WithPostNodeHook(func(ctx context.Context, name string, s MyState) {
        log.Printf("[POST] node=%-12s value=%d", name, s.Value)
    }).
    // Error Hook：节点执行失败（或重试耗尽仍失败后）触发（错误告警、闭合 Span 等）
    WithErrorHook(func(ctx context.Context, name string, s MyState, err error) {
        log.Printf("[ERR]  node=%-12s err=%v", name, err)
    })

thread, err := engine.Start(ctx, "start", MyState{Query: "你好"})
```
> 注意：若图中存在并发分支，上述钩子可能被多个 goroutine 并发调用，请确保钩子内部实现线程安全。

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

#### 5. 状态持久化 (Checkpointer) 示例

##### A. 基于 JSON 本地文件的 Checkpointer
```go
// 创建文件存储器，指定存放状态文件的目录
fc, _ := GopherGraph.NewFileCheckpointer[MyState]("./checkpoints")

// 在遇到中断挂起时，将 thread 保存到磁盘
sessionID := "user-session-123"
fc.Save(context.Background(), sessionID, thread)

// 重启程序后，可以从磁盘重新加载进度并恢复执行
loadedThread, _ := fc.Load(context.Background(), sessionID)
thread, _ = cg.Resume(context.Background(), loadedThread, modifiedState)

// 任务彻底跑完后，清理磁盘上的存档文件
if thread.IsFinished {
    fc.Delete(context.Background(), sessionID)
}
```

##### B. 基于 SQLite 的多版本追加写入 (Append-Only) Checkpointer
```go
// 创建 SQLite 存储器，自动开启 WAL 高并发模式
sc, _ := GopherGraph.NewSQLiteCheckpointer[MyState]("./checkpoints.db")
defer sc.Close()

// 每次 Save 都会追加写入一个新的版本号 (v1, v2, v3...)
sessionID := "user-session-123"
sc.Save(ctx, sessionID, thread)

// Load 默认获取最新版本的线程快照
latestThread, _ := sc.Load(ctx, sessionID)

// LoadVersion 支持精确回溯并恢复指定历史版本的快照（版本重放/回滚）
v1Thread, _ := sc.LoadVersion(ctx, sessionID, 1)

// ListVersions 查询该 session 的所有历史版本元数据
versions, _ := sc.ListVersions(ctx, sessionID)

// PruneVersions 保留最近 N 个版本，自动清理过期历史
sc.PruneVersions(ctx, sessionID, 5)
```

##### C. 自定义数据库回调 (Engine.WithSnapshotSaver)
如果你希望将节点流转信息和快照直接存入自己的业务数据库（如 MySQL、PostgreSQL、Redis 等），可通过 `Engine.WithSnapshotSaver` 注册回调函数。每步路由计算与状态更新完成后，引擎会自动将包含步数、节点名、耗时（`Duration`）以及完整 `Thread` 的 `StepSnapshot[S]` 传给你：
```go
engine := GopherGraph.NewEngine(cg).
    WithSnapshotSaver(func(ctx context.Context, snap GopherGraph.StepSnapshot[MyState]) error {
        stateJSON, err := snap.ThreadJSON() // 一键序列化 Thread 为紧凑 JSON
        if err != nil {
            return err
        }

        // 将节点执行信息与快照写入用户自己的数据库（返回 error 会立即中止工作流）
        return saveToMyDB(ctx, WorkflowRecord{
            Step:       snap.Step,
            Node:       snap.Node,
            DurationMs: snap.Duration.Milliseconds(),
            NextNode:   snap.Thread.NextNode,
            IsPaused:   snap.Thread.IsPaused,
            IsFinished: snap.Thread.IsFinished,
            ThreadJSON: stateJSON,
        })
    })
```

> 💡 持久化时机完全交由用户显式控制：你可以仅在 `thread.IsPaused` 时调用 `Checkpointer.Save` 以保持零运行时 I/O 开销，也可以通过 `Engine.WithSnapshotSaver` 在每步路由与状态定型后自动写入你自己的数据库。

#### 6. 条件目标校验与 Mermaid 流程图导出
使用 `AddConditionalEdgesWithTargets` 声明条件路由可能到达的目标节点（传入 `""` 表示可直接结束工作流），即可在 `Compile()` 时静态检验目标存在性，并通过 `Mermaid()` 导出完整流程图：
```go
g.AddConditionalEdgesWithTargets("reviewer", []string{"publisher", "translator", ""}, reviewRouter)

cg, err := g.Compile()
if err != nil {
    log.Fatal(err)
}

// 导出 Mermaid flowchart TD 字符串（也可调用 engine.Mermaid()）
fmt.Println(cg.Mermaid())
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

> **Q: Is GopherGraph reliable?**
> **A:** Like asking your girlfriend if she loves you — at least the compiler won't lie to you.

`GopherGraph` is a Code-as-Graph agent orchestration engine built in Go, inspired by Python's `LangGraph`. By leveraging Go 1.18+ Generics, GopherGraph ensures that workflow states are strictly typed at compile-time. Powered by native Goroutines and Channels, it executes agent communication with microsecond latency, making it the perfect engine for building complex looping agent workflows with Human-in-the-Loop (HITL), concurrent branches, and state persistence requirements.


### Key Features
- **Strictly Typed State:** Bind your custom struct to `Graph[S]` via Go generics. Say goodbye to unsafe `map[string]any` and runtime type assertions.
- **Support for Cycles/Loops:** Unlike traditional DAG (Directed Acyclic Graph) engines, GopherGraph natively supports loops and dynamic routing based on agent evaluation.
- **Parallel Branching with Short-Circuit Cancellation:** Run multiple agent nodes concurrently. Built on `context.WithCancel`, a failure in any branch immediately cancels all sibling goroutines, preventing wasted compute on LLM API calls.
- **Deep-Clone for Concurrency Safety:** Inject a custom `StateCloner` via `Engine.WithStateCloner` to safely deep-copy reference types (slices, maps, pointers) before fan-out, eliminating data races at the root.
- **Human-in-the-Loop (HITL):** Pause workflow execution *before* any designated node (including the start node), capture a snapshot (`Thread`), modify the state, and `Resume` seamlessly. Returns exported sentinel errors `ErrNotPaused` or `ErrAlreadyFinished` when resume pre-conditions are not met.
- **Infinite Loop Circuit Breaker:** Set a hard step limit with `Engine.WithMaxSteps`. Any accidental cycle in the graph returns a clear error instead of blocking forever.
- **Lightweight Lifecycle Hooks (Additive Chaining):** `Engine.WithPreNodeHook` / `WithPostNodeHook` / `WithErrorHook` fire across both sequential and parallel branch nodes without touching any `NodeFn` signature. Supports multiple chained registrations (additive) instead of overwriting, making it easy to decouple logging, tracing (OpenTelemetry), error alerting, and streaming.
- **Automatic Node Retry (Exponential Backoff):** Configure global or per-node retry policies with `Engine.WithRetryPolicy` and `Engine.WithNodeRetryPolicy` (max attempts, exponential backoff, and custom `RetryIf` predicates). Automatically skips `context.Canceled` / `context.DeadlineExceeded`, and pairs with `StateCloner` to isolate state across retry attempts.
- **Compile-Time & Runtime Structural Validation:** During `Compile()`, the engine validates node/edge existence, forbids interrupts on parallel target nodes, and strictly checks for outgoing edge conflicts on each node. Pair with `AddConditionalEdgesWithTargets` to validate conditional targets at compile time and block undeclared jumps at runtime.
- **Mermaid Flowchart Export:** Export deterministic `flowchart TD` diagrams via `CompiledGraph.Mermaid()` or `Engine.Mermaid()`, visually distinguishing standard nodes, interrupt nodes, conditional routes, and parallel fan-out/merge edges.
- **Explicit User-Controlled Persistence:** Generic `Checkpointer[S]` interface with built-in `FileCheckpointer[S]` (atomic file rename) and pure-Go `SQLiteCheckpointer[S]` (append-only multi-versioning, time travel history & pruning), featuring path traversal validation on `threadID`. Also provides `Engine.WithSnapshotSaver`, emitting a `StepSnapshot[S]` after every step with `Step`, `Node`, `Duration`, full `Thread`, and `ThreadJSON()` for saving directly into your own database (MySQL, PostgreSQL, Redis, etc.) with zero forced I/O overhead.
- **Pure Go & High-Performance (CGO-Free):** The core engine runs purely in memory over Go Goroutines and Channels, and built-in persistence (including SQLite) requires zero CGO dependencies.

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
    GopherGraph "github.com/unclesam-ly/GopherGraph"
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
    // Deep-copy state before fan-out or retries to prevent data races / state pollution
    WithStateCloner(func(s MyState) MyState {
        clone := s
        clone.Messages = append([]string{}, s.Messages...)
        return clone
    }).
    // Hard step limit — prevents accidental infinite loops
    WithMaxSteps(100).
    // Automatic retry with exponential backoff for transient errors (e.g., LLM rate limits)
    WithRetryPolicy(GopherGraph.RetryPolicy{
        MaxAttempts:    3,
        InitialBackoff: 100 * time.Millisecond,
        MaxBackoff:     2 * time.Second,
        Multiplier:     2.0,
    }).
    // Override retry policy for a specific node
    WithNodeRetryPolicy("llm_call", GopherGraph.RetryPolicy{
        MaxAttempts:    5,
        InitialBackoff: 200 * time.Millisecond,
    }).
    // Pre-hook: fires before each node (logging, tracing, streaming notifications)
    WithPreNodeHook(func(ctx context.Context, name string, s MyState) {
        log.Printf("[PRE]  node=%-12s query=%q", name, s.Query)
    }).
    // Post-hook: fires after each node (metrics, snapshots)
    WithPostNodeHook(func(ctx context.Context, name string, s MyState) {
        log.Printf("[POST] node=%-12s value=%d", name, s.Value)
    }).
    // Error-hook: fires when a node fails (after retries are exhausted)
    WithErrorHook(func(ctx context.Context, name string, s MyState, err error) {
        log.Printf("[ERR]  node=%-12s err=%v", name, err)
    })

thread, err := engine.Start(ctx, "start", MyState{Query: "hello"})
```
> Note: When parallel branches are present, lifecycle hooks may be invoked concurrently by multiple goroutines — ensure your hook implementations are concurrency-safe.

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

##### A. JSON File Checkpointer
```go
// Initialize a local directory storage
fc, _ := GopherGraph.NewFileCheckpointer[MyState]("./checkpoints")

// Save the thread snapshot when paused
sessionID := "user-session-123"
fc.Save(context.Background(), sessionID, thread)

// Reload the snapshot (e.g. after a process restart) and resume
loadedThread, _ := fc.Load(context.Background(), sessionID)
thread, _ = cg.Resume(context.Background(), loadedThread, modifiedState)

// Clean up the storage file once the workflow is completely finished
if thread.IsFinished {
    fc.Delete(context.Background(), sessionID)
}
```

##### B. SQLite Append-Only Multi-Version Checkpointer
```go
// Initialize SQLite checkpointer (WAL mode enabled automatically)
sc, _ := GopherGraph.NewSQLiteCheckpointer[MyState]("./checkpoints.db")
defer sc.Close()

// Append a new version snapshot on every Save (v1, v2, v3...)
sessionID := "user-session-123"
sc.Save(ctx, sessionID, thread)

// Load gets the latest version by default
latestThread, _ := sc.Load(ctx, sessionID)

// LoadVersion fetches a specific historical version for replay/rollback
v1Thread, _ := sc.LoadVersion(ctx, sessionID, 1)

// ListVersions retrieves version metadata for the thread
versions, _ := sc.ListVersions(ctx, sessionID)

// PruneVersions retains the N most recent versions and purges older records
sc.PruneVersions(ctx, sessionID, 5)
```

##### C. Custom Database Callback (Engine.WithSnapshotSaver)
If you want to store node execution details and thread snapshots directly in your own database (e.g., MySQL, PostgreSQL, Redis), register a callback via `Engine.WithSnapshotSaver`. After each step finishes routing and updates `Thread`, the engine delivers a `StepSnapshot[S]` containing the step index, node name, execution duration (`Duration`), and full `Thread`:
```go
engine := GopherGraph.NewEngine(cg).
    WithSnapshotSaver(func(ctx context.Context, snap GopherGraph.StepSnapshot[MyState]) error {
        stateJSON, err := snap.ThreadJSON() // Marshal Thread to compact JSON
        if err != nil {
            return err
        }

        // Persist node execution metadata & snapshot to your own DB (returning an error halts the workflow)
        return saveToMyDB(ctx, WorkflowRecord{
            Step:       snap.Step,
            Node:       snap.Node,
            DurationMs: snap.Duration.Milliseconds(),
            NextNode:   snap.Thread.NextNode,
            IsPaused:   snap.Thread.IsPaused,
            IsFinished: snap.Thread.IsFinished,
            ThreadJSON: stateJSON,
        })
    })
```

> 💡 Persistence timing is explicitly controlled by your application: save only when `thread.IsPaused` via `Checkpointer.Save` for zero runtime I/O overhead, or use `Engine.WithSnapshotSaver` to persist every step into your own database once routing and thread flags are finalized.

#### 6. Conditional Target Validation & Mermaid Flowchart Export
Declare reachable destinations for a conditional router with `AddConditionalEdgesWithTargets` (include `""` if the router can terminate the workflow). Targets are validated during `Compile()` and rendered into a complete Mermaid flowchart:
```go
g.AddConditionalEdgesWithTargets("reviewer", []string{"publisher", "translator", ""}, reviewRouter)

cg, err := g.Compile()
if err != nil {
    log.Fatal(err)
}

// Export a Mermaid flowchart TD string (also available via engine.Mermaid())
fmt.Println(cg.Mermaid())
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

> **Q: GopherGraph は使いやすいですか？**
> **A:** 彼女に「私のことが好き？」と聞くのと同じです——少なくともコンパイラは嘘をつきません。

`GopherGraph` は、Python エコシステムの `LangGraph` に着想を得て開発された、Go 言語向けの Code-as-Graph 型エージェントオーケストレーションエンジンです。Go 1.18+ のジェネリクスを活用することで、ワークフローの状態（State）をコンパイル時に厳密に型定義できます。Go 純正の Goroutines と Channels を利用したメモリ内メッセージングにより、高い並行性と極めて低いレイテンシを実現しています。自律的なループ（Looping）や、並行処理（Parallel Execution）、人間参加型（Human-in-the-Loop）の意思決定を伴うエージェントワークフローの開発に最適です。


### 主な特徴
- **型安全な状態管理:** ジェネリクスを用いて `Graph[S]` に構造体をバインドします。冗長な `map[string]any` やランタイム時の型アサーションから解放されます。
- **ループと条件付き分岐のサポート:** 従来の DAG（有向非巡回グラフ）の制限を超え、エージェントの判定に基づく条件付きルートや双方向のループをサポート。
- **並行処理とショートサーキットキャンセル:** `context.WithCancel` に基づく並行ブランチ実行を実装。いずれかのブランチでエラーが発生した瞬間に他の全 goroutine へキャンセルシグナルを伝播し、LLM API の無駄な呼び出しを防ぎます。
- **データ競合を防ぐ深いコピー（DeepClone）:** `Engine.WithStateCloner` でカスタムの深コピー関数を注入。参照型（スライス・マップ・ポインタ）を持つ状態でも、並行ブランチでのデータ競合を根本から解消します。
- **Human-in-the-Loop (HITL) の一時停止と再開:** 特定のノード（開始ノードを含む）の実行前に処理を自動停止し、スナップショット（`Thread`）を返します。承認や状態修正の後にシームレスに処理を再開（`Resume`）できます。再開判定用に `ErrNotPaused` や `ErrAlreadyFinished` といった明確なセンチネルエラーを提供しています。
- **無限ループのサーキットブレーカー:** `Engine.WithMaxSteps` でステップ数の上限を設定。グラフ内に意図しないサイクルが発生しても、永久ブロックせず明確なエラーを返します。
- **軽量なライフサイクルフック（複数チェーン登録対応）:** `Engine.WithPreNodeHook` / `WithPostNodeHook` / `WithErrorHook` により、`NodeFn` のシグネチャを変更することなく順次ノードおよび並行ブランチの成功・失敗ライフサイクル全体をフックできます。**複数回呼び出すことで複数のフックを順番に実行（チェーン蓄積）**可能です。
- **ノードの自動リトライ（指数バックオフ）:** `Engine.WithRetryPolicy`（全体）および `Engine.WithNodeRetryPolicy`（ノード個別）で最大試行回数、指数バックオフ、カスタム `RetryIf` フィルタを設定可能。`context.Canceled` / `context.DeadlineExceeded` 時は即座に中断し、`StateCloner` と組み合わせてリトライ間の状態分離を保証します。
- **コンパイル時および実行時の構造検証:** `Compile()` 実行時に、ノード・辺の存在チェック、並行ターゲットノードへの Interrupt 設定禁止、同一ノードの出辺タイプ競合を厳密に検出します。さらに `AddConditionalEdgesWithTargets` により条件付き分岐のターゲットをコンパイル時に静的検証し、実行時の未宣言ノードへの遷移もブロックします。
- **Mermaid フローチャート出力:** `CompiledGraph.Mermaid()` / `Engine.Mermaid()` により、決定論的（Deterministic）な `flowchart TD` 形式の図を出力できます。通常ノード、Interrupt ノード、条件付き分岐、並行ファンアウト・マージを視覚的に区別して描画します。
- **ユーザー主導の明示的な状態永続化:** 抽象的な `Checkpointer[S]` インタフェースと、アトミック書き込み対応の `FileCheckpointer[S]`、およびマルチバージョン追記（Append-Only）・履歴復元に対応した純 Go 製 `SQLiteCheckpointer[S]` を内蔵し、`threadID` のパス・トラバーサル検証を備えています。さらに `Engine.WithSnapshotSaver` コールバックにより、各ステップ終了時に `Step`・`Node`・`Duration`（実行時間）・完全な `Thread`・`ThreadJSON()` を含む `StepSnapshot[S]` を受け取り、独自の MySQL / PostgreSQL / Redis などへ直接保存できます（暗黙的な I/O オーバーヘッドを強制しません）。
- **ピュア Go & 高性能（CGO 不要）:** エンジンコアは Go 純正の Goroutines / Channels によるメモリ内処理で高速に動作し、内蔵永続化（SQLite を含む）も CGO 依存ゼロのピュア Go で実装されています。

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
    GopherGraph "github.com/unclesam-ly/GopherGraph"
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
    // ファンアウトやリトライ前に参照型を深くコピーしてデータ競合・状態汚染を排除
    WithStateCloner(func(s MyState) MyState {
        clone := s
        clone.Messages = append([]string{}, s.Messages...)
        return clone
    }).
    // ステップ上限 — 意図しない無限ループを防止
    WithMaxSteps(100).
    // 自動リトライ — 一時的なエラーに対する指数バックオフリトライを設定
    WithRetryPolicy(GopherGraph.RetryPolicy{
        MaxAttempts:    3,
        InitialBackoff: 100 * time.Millisecond,
        MaxBackoff:     2 * time.Second,
        Multiplier:     2.0,
    }).
    // 特定ノード向けに個別のリトライポリシーを上書き設定可能
    WithNodeRetryPolicy("llm_call", GopherGraph.RetryPolicy{
        MaxAttempts:    5,
        InitialBackoff: 200 * time.Millisecond,
    }).
    // Pre フック: 各ノード実行前に発火（ロギング・トレーシング・ストリーミング通知）
    WithPreNodeHook(func(ctx context.Context, name string, s MyState) {
        log.Printf("[PRE]  node=%-12s query=%q", name, s.Query)
    }).
    // Post フック: 各ノード実行後に発火（メトリクス・スナップショット）
    WithPostNodeHook(func(ctx context.Context, name string, s MyState) {
        log.Printf("[POST] node=%-12s value=%d", name, s.Value)
    }).
    // Error フック: ノード失敗時（リトライ上限到達後）に発火（アラート・Span 終了など）
    WithErrorHook(func(ctx context.Context, name string, s MyState, err error) {
        log.Printf("[ERR]  node=%-12s err=%v", name, err)
    })

thread, err := engine.Start(ctx, "start", MyState{Query: "こんにちは"})
```
> 注意：並行ブランチが存在する場合、各種フックは複数の goroutine から同時に呼び出される可能性があります。フック内の実装はスレッドセーフにしてください。

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

##### A. JSON ファイル Checkpointer
```go
// 保存先ディレクトリを指定してローカルファイルチェッカーを初期化
fc, _ := GopherGraph.NewFileCheckpointer[MyState]("./checkpoints")

// 一時停止時にスレッドスナップショットをファイルに保存
sessionID := "user-session-123"
fc.Save(context.Background(), sessionID, thread)

// スナップショットを読み込み、プロセス再起動後に処理を再開
loadedThread, _ := fc.Load(context.Background(), sessionID)
thread, _ = cg.Resume(context.Background(), loadedThread, modifiedState)

// 処理が完全に終了した後、ストレージ上のファイルをクリーンアップする
if thread.IsFinished {
    fc.Delete(context.Background(), sessionID)
}
```

##### B. SQLite マルチバージョン追記 (Append-Only) Checkpointer
```go
// SQLite チェッカーを初期化（WAL 高並行モードが自動有効化）
sc, _ := GopherGraph.NewSQLiteCheckpointer[MyState]("./checkpoints.db")
defer sc.Close()

// Save のたびに新しいバージョン（v1, v2, v3...）を追記保存
sessionID := "user-session-123"
sc.Save(ctx, sessionID, thread)

// Load はデフォルトで最新バージョンのスナップショットを取得
latestThread, _ := sc.Load(ctx, sessionID)

// LoadVersion は特定バージョンを正確に復元（タイムトラベル / 状態ロールバック）
v1Thread, _ := sc.LoadVersion(ctx, sessionID, 1)

// ListVersions で全履歴のメタデータ一覧を取得
versions, _ := sc.ListVersions(ctx, sessionID)

// PruneVersions で最新 N 件のみを保持し、古い履歴を削除
sc.PruneVersions(ctx, sessionID, 5)
```

##### C. カスタム DB コールバック (Engine.WithSnapshotSaver)
ノードの実行情報やスナップショットを独自の業務データベース（MySQL、PostgreSQL、Redis など）へ直接保存したい場合は、`Engine.WithSnapshotSaver` でコールバックを登録できます。各ステップのルーティング計算と `Thread` 状態更新の完了後、ステップ番号・ノード名・実行時間（`Duration`）・完全な `Thread` を含む `StepSnapshot[S]` が渡されます：
```go
engine := GopherGraph.NewEngine(cg).
    WithSnapshotSaver(func(ctx context.Context, snap GopherGraph.StepSnapshot[MyState]) error {
        stateJSON, err := snap.ThreadJSON() // Thread をコンパクト JSON へ一括シリアライズ
        if err != nil {
            return err
        }

        // ノード実行情報とスナップショットを独自 DB へ保存（error を返すと即座に停止）
        return saveToMyDB(ctx, WorkflowRecord{
            Step:       snap.Step,
            Node:       snap.Node,
            DurationMs: snap.Duration.Milliseconds(),
            NextNode:   snap.Thread.NextNode,
            IsPaused:   snap.Thread.IsPaused,
            IsFinished: snap.Thread.IsFinished,
            ThreadJSON: stateJSON,
        })
    })
```

> 💡 永続化のタイミングはアプリケーション側で明示的に制御できます：`thread.IsPaused` 時のみ `Checkpointer.Save` を呼んで実行時 I/O オーバーヘッドをゼロに保つことも、`Engine.WithSnapshotSaver` でステップ確定後に独自データベースへ自動保存することも可能です。

#### 6. 条件付きターゲット検証と Mermaid フローチャート出力
`AddConditionalEdgesWithTargets` で条件ルーターが遷移し得るターゲットノードを宣言しておくことで（ワークフロー終了を含む場合は `""` を指定）、`Compile()` 時の存在検証と完全な Mermaid フローチャート出力が可能になります：
```go
g.AddConditionalEdgesWithTargets("reviewer", []string{"publisher", "translator", ""}, reviewRouter)

cg, err := g.Compile()
if err != nil {
    log.Fatal(err)
}

// Mermaid flowchart TD 文字列を出力（engine.Mermaid() も利用可能）
fmt.Println(cg.Mermaid())
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
