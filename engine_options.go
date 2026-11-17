package GopherGraph

import (
	"context"
	"maps"
	"sync"
	"time"
)

// RetryPolicy 定义节点执行失败时的自动重试策略（适用于 LLM API 限流、网络抖动等瞬态错误）。
type RetryPolicy struct {
	MaxAttempts    int              // 最大尝试次数（包含首次执行；<= 1 表示不重试）
	InitialBackoff time.Duration    // 首次重试前的退避等待时间（<= 0 表示无等待立即重试）
	MaxBackoff     time.Duration    // 退避等待时间上限（<= 0 表示不限制上限）
	Multiplier     float64          // 指数退避乘数（<= 0 时默认按 2.0 计算）
	RetryIf        func(error) bool // 可选：自定义错误过滤函数，返回 true 才重试；若为 nil 则默认对除 Context 取消/超时外的所有错误重试
}

// Engine 是 CompiledGraph 的增强包装器，在完全兼容原有 API 的基础上，
// 提供以下生产级能力：
//
//  1. StateCloner        — 自定义深拷贝逻辑，消除并发分支与重试时的数据竞争隐患
//  2. MaxSteps           — 步数硬性熔断，防止图中的环路导致无限循环
//  3. PreNodeHook        — 节点执行前的生命周期钩子（日志、链路追踪等）
//  4. PostNodeHook       — 节点执行后的生命周期钩子（指标上报、流式推送等）
//  5. ErrorHook          — 节点执行失败时的错误钩子（告警、Span 异常闭合等）
//  6. RetryPolicy        — 全局或节点级的指数退避自动重试策略
//
// 所有选项均为可选，通过链式调用注册。未注册的选项将退回到 CompiledGraph 的默认行为。
type Engine[S any] struct {
	mu           sync.RWMutex
	graph        *CompiledGraph[S]
	stateCloner  func(S) S
	maxSteps     int
	preNodeHook  HookFn[S]
	postNodeHook HookFn[S]
	errorHook    ErrorHookFn[S]
	retryPolicy  RetryPolicy
	nodeRetries  map[string]RetryPolicy
}

// NewEngine 用一个已编译的图创建 Engine 实例。
func NewEngine[S any](graph *CompiledGraph[S]) *Engine[S] {
	return &Engine[S]{graph: graph}
}

// WithStateCloner 注册自定义深拷贝函数。
//
// 当图中存在并发分支（AddParallelEdges）或节点重试时，引擎会在启动每个并发 goroutine
// 或每次重试尝试之前调用此函数对共享状态进行深拷贝，从根本上消除引用类型（切片、map、指针）的数据竞争与状态污染。
func (e *Engine[S]) WithStateCloner(fn func(S) S) *Engine[S] {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.stateCloner = fn
	return e
}

// WithMaxSteps 设置引擎单次运行（Start/Resume）允许的最大节点执行步数。
//
// 这是防御图中存在意外环路（infinite loop）的最后一道硬性熔断。
// 超过上限后，引擎将返回明确的错误，而不是永久阻塞。
// 传入 0 或负数表示不限制（与不调用此方法等效）。
func (e *Engine[S]) WithMaxSteps(n int) *Engine[S] {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.maxSteps = n
	return e
}

// WithPreNodeHook 注册在每个节点执行【之前】触发的钩子函数。
//
// 支持多次调用：每次调用将新钩子追加到执行链末尾，按注册顺序依次执行，不覆盖已有钩子。
// 注意：当图中存在并发分支时，钩子可能被多个 goroutine 并发调用，请确保钩子内部实现是并发安全的。
func (e *Engine[S]) WithPreNodeHook(fn HookFn[S]) *Engine[S] {
	if fn == nil {
		return e
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.preNodeHook == nil {
		e.preNodeHook = fn
	} else {
		prev := e.preNodeHook
		e.preNodeHook = func(ctx context.Context, name string, s S) {
			prev(ctx, name, s)
			fn(ctx, name, s)
		}
	}
	return e
}

// WithPostNodeHook 注册在每个节点执行【之后】触发的钩子函数。
//
// 支持多次调用：每次调用将新钩子追加到执行链末尾，按注册顺序依次执行，不覆盖已有钩子。
// 注意：当图中存在并发分支时，钩子可能被多个 goroutine 并发调用，请确保钩子内部实现是并发安全的。
func (e *Engine[S]) WithPostNodeHook(fn HookFn[S]) *Engine[S] {
	if fn == nil {
		return e
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.postNodeHook == nil {
		e.postNodeHook = fn
	} else {
		prev := e.postNodeHook
		e.postNodeHook = func(ctx context.Context, name string, s S) {
			prev(ctx, name, s)
			fn(ctx, name, s)
		}
	}
	return e
}

// WithErrorHook 注册在节点执行【失败】（若开启重试，则为重试耗尽仍失败后）触发的错误钩子函数。
//
// 支持多次调用：每次调用将新钩子追加到执行链末尾，按注册顺序依次执行，不覆盖已有钩子。
// 注意：当图中存在并发分支时，钩子可能被多个 goroutine 并发调用，请确保钩子内部实现是并发安全的。
func (e *Engine[S]) WithErrorHook(fn ErrorHookFn[S]) *Engine[S] {
	if fn == nil {
		return e
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.errorHook == nil {
		e.errorHook = fn
	} else {
		prev := e.errorHook
		e.errorHook = func(ctx context.Context, name string, s S, err error) {
			prev(ctx, name, s, err)
			fn(ctx, name, s, err)
		}
	}
	return e
}

// WithRetryPolicy 设置全局默认的节点失败自动重试策略。
// 若某个节点同时通过 WithNodeRetryPolicy 设置了专属策略，则优先使用节点专属策略。
func (e *Engine[S]) WithRetryPolicy(policy RetryPolicy) *Engine[S] {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.retryPolicy = policy
	return e
}

// WithNodeRetryPolicy 为指定名称的节点设置专属的自动重试策略（优先级高于全局 WithRetryPolicy）。
func (e *Engine[S]) WithNodeRetryPolicy(nodeName string, policy RetryPolicy) *Engine[S] {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.nodeRetries == nil {
		e.nodeRetries = make(map[string]RetryPolicy)
	}
	e.nodeRetries[nodeName] = policy
	return e
}

// opts 将 Engine 的配置快照打包成 runOptions，传入底层 run() 调度器。
// 使用 RLock 保证读取配置时的并发安全。
func (e *Engine[S]) opts() runOptions[S] {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return runOptions[S]{
		maxSteps:     e.maxSteps,
		stateCloner:  e.stateCloner,
		preNodeHook:  e.preNodeHook,
		postNodeHook: e.postNodeHook,
		errorHook:    e.errorHook,
		retryPolicy:  e.retryPolicy,
		nodeRetries:  maps.Clone(e.nodeRetries),
	}
}

// Start 从指定节点启动工作流，行为与 CompiledGraph.Start 完全一致，
// 但会附加所有已注册的 Engine 增强选项（Hooks、MaxSteps、StateCloner、RetryPolicy）。
func (e *Engine[S]) Start(ctx context.Context, startNode string, initialState S) (*Thread[S], error) {
	return e.graph.start(ctx, startNode, initialState, e.opts())
}

// Resume 恢复执行一个被暂停的线程，行为与 CompiledGraph.Resume 完全一致，
// 但会附加所有已注册的 Engine 增强选项（Hooks、MaxSteps、StateCloner、RetryPolicy）。
// 复用 validateResume 进行前提条件校验，与 CompiledGraph.Resume 保持一致。
func (e *Engine[S]) Resume(ctx context.Context, thread *Thread[S], modifiedState S) (*Thread[S], error) {
	if err := validateResume(thread); err != nil {
		return nil, err
	}

	thread.State = modifiedState
	thread.IsPaused = false

	return e.graph.run(ctx, thread, e.opts())
}
