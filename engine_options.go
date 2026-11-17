package GopherGraph

import (
	"context"
	"encoding/json"
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

// StepSnapshot 承载单步执行完毕后的完整节点信息、耗时与线程快照，供用户自定义落库保存。
type StepSnapshot[S any] struct {
	Step     int           `json:"step"`     // 当前已执行的步数序号（从 1 开始递增；若首节点启动前即触发中断则为 0）
	Node     string        `json:"node"`     // 刚刚执行完毕的节点名称（若首节点启动前即触发中断则为空 ""）
	Duration time.Duration `json:"duration"` // 当前步（含节点执行、重试及并发分支合并）的执行耗时
	Thread   Thread[S]     `json:"thread"`   // 完整的线程快照（含最新 State、已算好的 NextNode、IsPaused、IsFinished）
}

// ThreadJSON 将快照中的 Thread 序列化为紧凑 JSON 字符串，方便直接写入自定义数据库的 TEXT/JSON 字段。
// 后续恢复时只需 json.Unmarshal 到 Thread[S] 即可直接传给 Engine.Resume。
func (s StepSnapshot[S]) ThreadJSON() (string, error) {
	data, err := json.Marshal(s.Thread)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// SnapshotSaveFn 是用户自定义的节点与线程快照保存函数签名。
// 引擎在每一步路由计算及中断/完结状态更新完成后调用此函数。
// 若返回非 nil 错误（例如写入数据库失败），引擎将立即中止运行并将该错误向上返回。
type SnapshotSaveFn[S any] func(ctx context.Context, snapshot StepSnapshot[S]) error

// Engine 是 CompiledGraph 的增强包装器，在完全兼容原有 API 的基础上，
// 提供以下生产级能力：
//
//  1. StateCloner        — 自定义深拷贝逻辑，消除并发分支与重试时的数据竞争隐患
//  2. MaxSteps           — 步数硬性熔断，防止图中的环路导致无限循环
//  3. PreNodeHook        — 节点执行前的生命周期钩子（日志、链路追踪等）
//  4. PostNodeHook       — 节点执行后的生命周期钩子（指标上报、流式推送等）
//  5. ErrorHook          — 节点执行失败时的错误钩子（告警、Span 异常闭合等）
//  6. RetryPolicy        — 全局或节点级的指数退避自动重试策略
//  7. SnapshotSaver      — 每步完成后的完整快照持久化回调（对接用户自定义 MySQL/PG/Redis 等数据库）
//
// 所有选项均为可选，通过链式调用注册。未注册的选项将退回到 CompiledGraph 的默认行为。
type Engine[S any] struct {
	mu            sync.RWMutex
	graph         *CompiledGraph[S]
	stateCloner   func(S) S
	maxSteps      int
	preNodeHook   HookFn[S]
	postNodeHook  HookFn[S]
	errorHook     ErrorHookFn[S]
	retryPolicy   RetryPolicy
	nodeRetries   map[string]RetryPolicy
	snapshotSaver SnapshotSaveFn[S]
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

// WithSnapshotSaver 注册自定义的每步快照保存回调函数。
//
// 每当一个节点（含其后续的并发分支合并与条件路由计算）执行完毕并更新完 Thread 状态后，
// 引擎会将包含当前步数（Step）、刚执行的节点名（Node）、节点耗时（Duration）以及完整线程快照（Thread）
// 的 StepSnapshot 传递给此函数，方便用户将节点流转信息和状态保存到自己的数据库（如 MySQL、PostgreSQL、Redis）。
//
// 支持多次调用链式追加；若任一回调返回非 nil 错误，工作流将立即中止并返回该错误。
func (e *Engine[S]) WithSnapshotSaver(fn SnapshotSaveFn[S]) *Engine[S] {
	if fn == nil {
		return e
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.snapshotSaver == nil {
		e.snapshotSaver = fn
	} else {
		prev := e.snapshotSaver
		e.snapshotSaver = func(ctx context.Context, snap StepSnapshot[S]) error {
			if err := prev(ctx, snap); err != nil {
				return err
			}
			return fn(ctx, snap)
		}
	}
	return e
}

// opts 将 Engine 的配置快照打包成 runOptions，传入底层 run() 调度器。
// 使用 RLock 保证读取配置时的并发安全。
func (e *Engine[S]) opts() runOptions[S] {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return runOptions[S]{
		maxSteps:      e.maxSteps,
		stateCloner:   e.stateCloner,
		preNodeHook:   e.preNodeHook,
		postNodeHook:  e.postNodeHook,
		errorHook:     e.errorHook,
		retryPolicy:   e.retryPolicy,
		nodeRetries:   maps.Clone(e.nodeRetries),
		snapshotSaver: e.snapshotSaver,
	}
}

// Start 从指定节点启动工作流，行为与 CompiledGraph.Start 完全一致，
// 但会附加所有已注册的 Engine 增强选项（Hooks、MaxSteps、StateCloner、RetryPolicy、SnapshotSaver）。
func (e *Engine[S]) Start(ctx context.Context, startNode string, initialState S) (*Thread[S], error) {
	return e.graph.start(ctx, startNode, initialState, e.opts())
}

// Resume 恢复执行一个被暂停的线程，行为与 CompiledGraph.Resume 完全一致，
// 但会附加所有已注册的 Engine 增强选项（Hooks、MaxSteps、StateCloner、RetryPolicy、SnapshotSaver）。
// 复用 validateResume 进行前提条件校验，与 CompiledGraph.Resume 保持一致。
func (e *Engine[S]) Resume(ctx context.Context, thread *Thread[S], modifiedState S) (*Thread[S], error) {
	if err := validateResume(thread); err != nil {
		return nil, err
	}

	thread.State = modifiedState
	thread.IsPaused = false

	return e.graph.run(ctx, thread, e.opts())
}
