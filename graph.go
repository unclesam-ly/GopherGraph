package GopherGraph

import "context"

// NodeFn 代表图中的一个执行节点（Agent）。
// 它接收当前的上下文和状态 S，处理后返回更新后的状态 S，或者返回错误。
type NodeFn[S any] func(ctx context.Context, state S) (S, error)

// RouterFn 代表条件路由函数。
// 它根据当前状态 S，动态决定下一个要执行的节点名称（例如返回 "translator" 或 "reviewer"
type RouterFn[S any] func(ctx context.Context, state S) (string, error)

// ParallelMergeFn 接收分流前的原始状态 parent，以及各个并发分支运行结束后的状态列表 branches，返回合并后的状态。
type ParallelMergeFn[S any] func(ctx context.Context, parent S, branches []S) (S, error)

// parallelStep 内部结构，用于记录并发分支的配置
type parallelStep[S any] struct {
	targets []string           // 需要并发执行的目标节点列表
	next    string             // 合并后下一步要去的节点名称
	merger  ParallelMergeFn[S] // 状态合并函数
}

// Graph 是图的构建器，S 代表用户自定义的状态结构体。
type Graph[S any] struct {
	nodes       map[string]NodeFn[S]       // 节点名称 -> 节点执行函数
	edges       map[string]string          // 普通单向边：源节点 -> 目标节点
	conditional map[string]RouterFn[S]     // 条件路由边：源节点 -> 路由函数
	interrupts  map[string]bool            // 标记需要中断的节点：执行完该节点后挂起
	parallels   map[string]parallelStep[S] // 并发分支：源节点 -> 并发步骤配置
}

// NewGraph 创建并初始化一个全新强类型的图构建器
func NewGraph[S any]() *Graph[S] {
	return &Graph[S]{
		nodes:       make(map[string]NodeFn[S]),
		edges:       make(map[string]string),
		conditional: make(map[string]RouterFn[S]),
		interrupts:  make(map[string]bool),
		parallels:   make(map[string]parallelStep[S]), // 初始化 parallels
	}
}

// AddNode 向图中注册一个节点（Agent）
func (g *Graph[S]) AddNode(name string, fn NodeFn[S]) {
	g.nodes[name] = fn
}

// AddEdge 建立一条从 from 节点到 to 节点的静态连接线
func (g *Graph[S]) AddEdge(from, to string) {
	g.edges[from] = to
}

// AddConditionalEdges 建立一条从 from 节点出发的条件路由
// 到底去哪，由 router 函数在运行时根据当前 State 动态决定
func (g *Graph[S]) AddConditionalEdges(from string, router RouterFn[S]) {
	g.conditional[from] = router
}

// AddInterrupt 标记在执行 nodeName 节点“之前”进行中断挂起，等待人工确认
func (g *Graph[S]) AddInterrupt(nodeName string) {
	g.interrupts[nodeName] = true
}

// AddParallelEdges 建立并发分支。
// from: 起始节点
// targets: 需要并发执行的多个目标节点名称（如 "translate_en", "translate_fr"）
// next: 所有并发节点运行完并合并后，下一步要去的节点（如 "publisher"）
// merger: 用于合并各个并发分支状态的函数
func (g *Graph[S]) AddParallelEdges(from string, targets []string, next string, merger ParallelMergeFn[S]) {
	g.parallels[from] = parallelStep[S]{
		targets: targets,
		next:    next,
		merger:  merger,
	}
}

// HookFn 是节点执行前后的生命周期钩子函数签名。
// nodeName 为当前执行的节点名称，state 为该节点执行时的状态快照（只读语义，请勿修改）。
// 可通过此钩子接入日志、链路追踪（OpenTelemetry）、监控指标等能力，而无需修改任何 NodeFn。
type HookFn[S any] func(ctx context.Context, nodeName string, state S)
