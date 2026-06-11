package GopherGraph

import "context"

// Engine 是 CompiledGraph 的增强包装器，在完全兼容原有 API 的基础上，
// 提供以下四项生产级能力：
//
//  1. StateCloner    — 自定义深拷贝逻辑，消除并发分支的数据竞争隐患
//  2. MaxSteps       — 步数硬性熔断，防止图中的环路导致无限循环
//  3. PreNodeHook    — 节点执行前的生命周期钩子（日志、链路追踪等）
//  4. PostNodeHook   — 节点执行后的生命周期钩子（指标上报、流式推送等）
//
// 所有选项均为可选，通过链式调用注册。未注册的选项将退回到 CompiledGraph 的默认行为。
//
// 用法示例：
//
//	engine := GopherGraph.NewEngine(compiledGraph).
//	    WithStateCloner(func(s MyState) MyState {
//	        clone := s
//	        clone.Messages = append([]string{}, s.Messages...)
//	        return clone
//	    }).
//	    WithMaxSteps(100).
//	    WithPreNodeHook(func(ctx context.Context, name string, s MyState) {
//	        log.Printf("[PRE]  node=%s", name)
//	    }).
//	    WithPostNodeHook(func(ctx context.Context, name string, s MyState) {
//	        log.Printf("[POST] node=%s value=%+v", name, s)
//	    })
//
//	thread, err := engine.Start(ctx, "start", initialState)
type Engine[S any] struct {
	graph        *CompiledGraph[S]
	stateCloner  func(S) S
	maxSteps     int
	preNodeHook  HookFn[S]
	postNodeHook HookFn[S]
}

// NewEngine 用一个已编译的图创建 Engine 实例。
func NewEngine[S any](graph *CompiledGraph[S]) *Engine[S] {
	return &Engine[S]{graph: graph}
}

// WithStateCloner 注册自定义深拷贝函数。
//
// 当图中存在并发分支（AddParallelEdges）时，引擎会在启动每个并发 goroutine 之前
// 调用此函数对共享状态进行深拷贝，从根本上消除引用类型（切片、map、指针）的数据竞争。
//
// 若不注册，引擎退化为 Go 默认的值拷贝语义（浅拷贝）。
// 对于状态中含有引用类型的场景，强烈建议注册此函数。
func (e *Engine[S]) WithStateCloner(fn func(S) S) *Engine[S] {
	e.stateCloner = fn
	return e
}

// WithMaxSteps 设置引擎单次运行（Start/Resume）允许的最大节点执行步数。
//
// 这是防御图中存在意外环路（infinite loop）的最后一道硬性熔断。
// 超过上限后，引擎将返回明确的错误，而不是永久阻塞。
//
// 建议根据工作流的预期最大深度合理设置（如 100~1000）。
// 传入 0 或负数表示不限制（与不调用此方法等效）。
func (e *Engine[S]) WithMaxSteps(n int) *Engine[S] {
	e.maxSteps = n
	return e
}

// WithPreNodeHook 注册在每个节点执行【之前】触发的钩子函数。
//
// 典型用途：
//   - 打印结构化日志（node 名称、输入状态）
//   - 记录 OpenTelemetry Span 的开始时间
//   - 向前端推送"正在执行 xxx 节点"的流式事件
//
// 钩子函数接收到的 state 是节点执行【前】的状态快照，请勿修改（只读语义）。
// 钩子中的 panic 会向上传播，建议在内部做好 recover。
func (e *Engine[S]) WithPreNodeHook(fn HookFn[S]) *Engine[S] {
	e.preNodeHook = fn
	return e
}

// WithPostNodeHook 注册在每个节点执行【之后】触发的钩子函数。
//
// 典型用途：
//   - 打印节点的输出状态（用于 Debug）
//   - 上报节点耗时指标（Prometheus/DataDog）
//   - 向前端推送节点执行完毕的流式事件
//
// 钩子函数接收到的 state 是节点执行【后】更新过的状态，同为只读语义。
func (e *Engine[S]) WithPostNodeHook(fn HookFn[S]) *Engine[S] {
	e.postNodeHook = fn
	return e
}

// opts 将 Engine 的配置打包成 runOptions，传入底层 run() 调度器。
func (e *Engine[S]) opts() runOptions[S] {
	return runOptions[S]{
		maxSteps:     e.maxSteps,
		stateCloner:  e.stateCloner,
		preNodeHook:  e.preNodeHook,
		postNodeHook: e.postNodeHook,
	}
}

// Start 从指定节点启动工作流，行为与 CompiledGraph.Start 完全一致，
// 但会附加所有已注册的 Engine 增强选项（Hooks、MaxSteps、StateCloner）。
func (e *Engine[S]) Start(ctx context.Context, startNode string, initialState S) (*Thread[S], error) {
	thread := &Thread[S]{
		State:    initialState,
		NextNode: startNode,
	}
	return e.graph.run(ctx, thread, e.opts())
}

// Resume 恢复执行一个被暂停的线程，行为与 CompiledGraph.Resume 完全一致，
// 但会附加所有已注册的 Engine 增强选项（Hooks、MaxSteps、StateCloner）。
func (e *Engine[S]) Resume(ctx context.Context, thread *Thread[S], modifiedState S) (*Thread[S], error) {
	if !thread.IsPaused {
		return nil, errNotPaused
	}
	if thread.IsFinished {
		return nil, errAlreadyFinished
	}

	thread.State = modifiedState
	thread.IsPaused = false

	return e.graph.run(ctx, thread, e.opts())
}

// 复用与 CompiledGraph.Resume 中相同的哨兵错误，避免魔法字符串重复
var (
	errNotPaused       = errStr("cannot resume: thread is not paused")
	errAlreadyFinished = errStr("cannot resume: thread is already finished")
)

type errStr string

func (e errStr) Error() string { return string(e) }
