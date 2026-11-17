package GopherGraph

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
)

// Thread 代表图执行的具体实例快照（线程上下文）
// 它可以被序列化并存储，用来实现中断后的状态恢复
type Thread[S any] struct {
	State      S      // 当前共享的强类型状态数据
	NextNode   string // 下一步即将执行的节点名称
	IsPaused   bool   // 是否因为触发中断而处于暂停状态
	IsFinished bool   // 工作流是否全部结束（即到达了终点）
}

// CompiledGraph 是编译后的只读图，准备好投入运行。
type CompiledGraph[S any] struct {
	nodes              map[string]NodeFn[S]
	edges              map[string]string
	conditional        map[string]RouterFn[S]
	conditionalTargets map[string][]string
	interrupts         map[string]bool
	parallels          map[string]parallelStep[S]
}

// Compile 校验图结构的合法性，并生成可运行的 CompiledGraph。
func (g *Graph[S]) Compile() (*CompiledGraph[S], error) {
	// 首先展示所有注册阶段积累的错误（如重复注册节点或边）
	if err := errorsJoin(g.errs); err != nil {
		return nil, fmt.Errorf("graph registration errors:\n%w", err)
	}

	if len(g.nodes) == 0 {
		return nil, errors.New("cannot compile graph: graph contains no nodes")
	}

	// 校验静态边 (From -> To) 的起始和目标节点是否存在
	for from, to := range g.edges {
		if _, exists := g.nodes[from]; !exists {
			return nil, fmt.Errorf("compile error: edge origin %q does not exist", from)
		}
		if _, exists := g.nodes[to]; !exists {
			return nil, fmt.Errorf("compile error: edge destination %q does not exist", to)
		}
	}

	// 校验条件路由边的源节点是否存在
	for from := range g.conditional {
		if _, exists := g.nodes[from]; !exists {
			return nil, fmt.Errorf("compile error: conditional edge origin %q does not exist", from)
		}
	}

	// 校验声明了目标列表的条件路由边，确保所有非空目标节点均存在
	for from, targets := range g.conditionalTargets {
		for _, target := range targets {
			if target != "" {
				if _, exists := g.nodes[target]; !exists {
					return nil, fmt.Errorf("compile error: conditional edge target %q from %q does not exist", target, from)
				}
			}
		}
	}

	// 校验被标记为中断的节点是否存在
	for node := range g.interrupts {
		if _, exists := g.nodes[node]; !exists {
			return nil, fmt.Errorf("compile error: interrupt node %q does not exist", node)
		}
	}

	// 校验并发连线边的有效性
	for from, step := range g.parallels {
		if _, exists := g.nodes[from]; !exists {
			return nil, fmt.Errorf("compile error: parallel edge origin %q does not exist", from)
		}
		if len(step.targets) == 0 {
			return nil, fmt.Errorf("compile error: parallel edge from %q has no targets", from)
		}

		for _, target := range step.targets {
			if _, exists := g.nodes[target]; !exists {
				return nil, fmt.Errorf("compile error: parallel target %q from %q does not exist", target, from)
			}
			if g.interrupts[target] {
				return nil, fmt.Errorf("compile error: parallel target %q from %q cannot be marked as an interrupt node", target, from)
			}
		}
		if step.next != "" {
			if _, exists := g.nodes[step.next]; !exists {
				return nil, fmt.Errorf("compile error: parallel next node %q from %q does not exist", step.next, from)
			}
		}
		if step.merger == nil {
			return nil, fmt.Errorf("compile error: parallel step from %q has nil merger", from)
		}
	}

	// 校验同一节点的出边冲突：不能同时存在于 edges、conditional、parallels 中
	for node := range g.nodes {
		conflictCount := 0
		if _, ok := g.edges[node]; ok {
			conflictCount++
		}
		if _, ok := g.conditional[node]; ok {
			conflictCount++
		}
		if _, ok := g.parallels[node]; ok {
			conflictCount++
		}
		if conflictCount > 1 {
			return nil, fmt.Errorf("compile error: node %q has conflicting outgoing edges (defined in multiple edge types simultaneously)", node)
		}
	}

	// 返回编译好的只读图（防御性拷贝，阻止编译后修改 Graph 对 CompiledGraph 造成污染）
	nodesCopy := make(map[string]NodeFn[S], len(g.nodes))
	for k, v := range g.nodes {
		nodesCopy[k] = v
	}
	edgesCopy := make(map[string]string, len(g.edges))
	for k, v := range g.edges {
		edgesCopy[k] = v
	}
	conditionalCopy := make(map[string]RouterFn[S], len(g.conditional))
	for k, v := range g.conditional {
		conditionalCopy[k] = v
	}
	interruptsCopy := make(map[string]bool, len(g.interrupts))
	for k, v := range g.interrupts {
		interruptsCopy[k] = v
	}
	parallelsCopy := make(map[string]parallelStep[S], len(g.parallels))
	for k, v := range g.parallels {
		parallelsCopy[k] = v
	}
	conditionalTargetsCopy := make(map[string][]string, len(g.conditionalTargets))
	for k, v := range g.conditionalTargets {
		conditionalTargetsCopy[k] = append([]string(nil), v...)
	}

	return &CompiledGraph[S]{
		nodes:              nodesCopy,
		edges:              edgesCopy,
		conditional:        conditionalCopy,
		conditionalTargets: conditionalTargetsCopy,
		interrupts:         interruptsCopy,
		parallels:          parallelsCopy,
	}, nil
}

// ErrNotPaused 在调用 Resume 时线程未处于暂停状态时返回。可用于 errors.Is 判断。
var ErrNotPaused = errors.New("cannot resume: thread is not paused")

// ErrAlreadyFinished 在调用 Resume 时线程已结束时返回。可用于 errors.Is 判断。
var ErrAlreadyFinished = errors.New("cannot resume: thread is already finished")

// validateResume 验证线程是否满足恢复执行的前提条件。
// CompiledGraph.Resume 和 Engine.Resume 共用此逻辑，避免重复实现。
func validateResume[S any](thread *Thread[S]) error {
	if !thread.IsPaused {
		return ErrNotPaused
	}
	if thread.IsFinished {
		return ErrAlreadyFinished
	}
	return nil
}

// Start 从指定的起始节点开始运行图，并持续流转，直到遇到中断或运行结束
func (cg *CompiledGraph[S]) Start(ctx context.Context, startNode string, initialState S) (*Thread[S], error) {
	return cg.start(ctx, startNode, initialState, runOptions[S]{})
}

// start 校验起始节点并处理首节点中断，随后进入主循环调度器
func (cg *CompiledGraph[S]) start(ctx context.Context, startNode string, initialState S, opts runOptions[S]) (*Thread[S], error) {
	if startNode == "" {
		return nil, errors.New("start error: startNode must not be empty")
	}
	if _, exists := cg.nodes[startNode]; !exists {
		return nil, fmt.Errorf("start error: startNode %q not found", startNode)
	}

	thread := &Thread[S]{
		State:    initialState,
		NextNode: startNode,
	}

	// 若起始节点本身被标记为中断点，则在执行该节点前直接挂起
	if cg.interrupts[startNode] {
		thread.IsPaused = true
		return thread, nil
	}

	return cg.run(ctx, thread, opts)
}

// Resume 恢复执行一个被暂停的线程，并允许注入外部修改后的状态数据（例如人工审批修改后的结果）
func (cg *CompiledGraph[S]) Resume(ctx context.Context, thread *Thread[S], modifiedState S) (*Thread[S], error) {
	if err := validateResume(thread); err != nil {
		return nil, err
	}

	// 注入人工修改后的状态，并解除暂停标记
	thread.State = modifiedState
	thread.IsPaused = false

	return cg.run(ctx, thread, runOptions[S]{})
}

// runOptions 封装引擎运行时的可选参数，由 Engine 包装器传入
type runOptions[S any] struct {
	maxSteps     int
	stateCloner  func(S) S
	preNodeHook  HookFn[S]
	postNodeHook HookFn[S]
}

// run 是引擎内部循环调度器，负责驱动节点向前流转
func (cg *CompiledGraph[S]) run(ctx context.Context, thread *Thread[S], opts runOptions[S]) (*Thread[S], error) {
	stepCount := 0

	for {
		// 【熔断 2】检查 Context，支持外部超时控制或取消
		select {
		case <-ctx.Done():
			return thread, ctx.Err()
		default:
		}

		currentNodeName := thread.NextNode
		// 如果下一个执行节点为空，说明整条流水线已运行结束
		if currentNodeName == "" {
			thread.IsFinished = true
			return thread, nil
		}

		// 【熔断 1】检查步数上限，防御死循环（infinite loop protection）
		if opts.maxSteps > 0 {
			if stepCount >= opts.maxSteps {
				return thread, fmt.Errorf(
					"engine halt: max steps (%d) exceeded, possible infinite loop in graph",
					opts.maxSteps,
				)
			}
			stepCount++
		}

		// 查找当前节点
		nodeFn, exists := cg.nodes[currentNodeName]
		if !exists {
			return thread, fmt.Errorf("runtime error: node %q not found", currentNodeName)
		}

		// 触发 PreNodeHook
		if opts.preNodeHook != nil {
			opts.preNodeHook(ctx, currentNodeName, thread.State)
		}

		// 执行当前节点
		newState, err := nodeFn(ctx, thread.State)
		if err != nil {
			return thread, fmt.Errorf("node %q execution error: %w", currentNodeName, err)
		}
		thread.State = newState

		// 触发 PostNodeHook
		if opts.postNodeHook != nil {
			opts.postNodeHook(ctx, currentNodeName, thread.State)
		}

		// 计算下一个该执行的节点名称
		var nextNode string
		if step, isParallel := cg.parallels[currentNodeName]; isParallel {
			// 【处理并发分流】使用短路取消机制
			branches, err := cg.runParallelBranches(ctx, step.targets, thread.State, opts)
			if err != nil {
				return thread, fmt.Errorf("parallel branch execution error: %w", err)
			}

			// 【状态合并】调用用户自定义的合并函数
			mergedState, err := step.merger(ctx, thread.State, branches)
			if err != nil {
				return thread, fmt.Errorf("parallel merger execution error: %w", err)
			}
			thread.State = mergedState
			nextNode = step.next
		} else if routerFn, ok := cg.conditional[currentNodeName]; ok {
			// 如果有条件路由函数，则通过路由函数动态计算去向
			next, err := routerFn(ctx, thread.State)
			if err != nil {
				return thread, fmt.Errorf("router for node %q execution error: %w", currentNodeName, err)
			}
			// 立即校验返回的节点是否存在，提供更清晰的报错
			if next != "" {
				if _, exists := cg.nodes[next]; !exists {
					return thread, fmt.Errorf("router for node %q returned non-existent node %q (not found)", currentNodeName, next)
				}
			}
			// 若注册时通过 AddConditionalEdgesWithTargets 声明了允许的目标范围，则校验是否越界
			if allowed, hasTargets := cg.conditionalTargets[currentNodeName]; hasTargets {
				if !slices.Contains(allowed, next) {
					return thread, fmt.Errorf("router for node %q returned undeclared target %q (allowed: %v)", currentNodeName, next, allowed)
				}
			}
			nextNode = next
		} else {
			// 否则使用静态连线边
			nextNode = cg.edges[currentNodeName]
		}

		// 更新快照中的"下一个节点"
		thread.NextNode = nextNode

		// 核心中断机制：如果即将进入的下一个节点被标记为了中断节点，则在此挂起
		if cg.interrupts[nextNode] {
			thread.IsPaused = true
			return thread, nil // 暂停执行，返回当前快照供外部人工介入
		}
	}
}

// runParallelBranches 并发执行多个目标节点，并实现首个错误的短路取消。
//
//   - 使用 context.WithCancel 派生子 Context，任意分支报错立即调用 cancel()，
//     通知其他仍在运行的分支通过 ctx.Done() 优雅退出，避免 AI 算力空转。
//   - 使用缓冲 channel（容量 = 分支数）收集结果，防止 goroutine 在写入时永久阻塞（goroutine 泄漏）。
//   - stateCloner 为可选的深拷贝函数；若为 nil，则传入值拷贝（对含引用类型的状态存在数据竞争风险，需用户知悉）。
func (cg *CompiledGraph[S]) runParallelBranches(
	ctx context.Context,
	targets []string,
	state S,
	opts runOptions[S],
) ([]S, error) {
	// 派生可取消的子 Context：任意分支出错时，立即广播取消信号
	cancelCtx, cancel := context.WithCancel(ctx)
	defer cancel() // 确保函数退出时释放资源，不论成功或失败

	type result struct {
		idx   int
		state S
		err   error
	}

	// 缓冲通道容量 = 分支数，确保所有 goroutine 均可不阻塞地写入结果
	resultCh := make(chan result, len(targets))

	var wg sync.WaitGroup
	for i, target := range targets {
		wg.Add(1)

		// 按需深拷贝状态：有 stateCloner 则深拷贝，否则值拷贝（Go 默认语义）
		var stateCopy S
		if opts.stateCloner != nil {
			stateCopy = opts.stateCloner(state)
		} else {
			stateCopy = state
		}

		go func(idx int, nodeName string, s S) {
			defer wg.Done()

			// 在执行真正的工作之前，先检查是否已被其他分支取消
			select {
			case <-cancelCtx.Done():
				resultCh <- result{idx: idx, err: cancelCtx.Err()}
				return
			default:
			}

			// 触发并发子节点的 PreNodeHook
			if opts.preNodeHook != nil {
				opts.preNodeHook(cancelCtx, nodeName, s)
			}

			nodeFn := cg.nodes[nodeName]
			resState, err := nodeFn(cancelCtx, s)
			if err != nil {
				cancel() // 关键：一处报错，立即取消其余所有兄弟 goroutine
				resultCh <- result{idx: idx, err: err}
				return
			}

			// 触发并发子节点的 PostNodeHook
			if opts.postNodeHook != nil {
				opts.postNodeHook(cancelCtx, nodeName, resState)
			}

			resultCh <- result{idx: idx, state: resState}
		}(i, target, stateCopy)
	}

	// 等待所有 goroutine 完成后关闭结果通道
	wg.Wait()
	close(resultCh)

	// 【P0 修复】先收集所有结果，再判断是否有真实错误。
	// 原先在循环中遇到非 context.Canceled 错误就立即 return，但由于 channel 中的写入顺序是不确定的，
	// 存在以下竞态：分支 A 先把 context.Canceled 写入 channel，紧接着分支 B 才把真实错误写入。
	// 若此时早返回，真实错误永远不会被读取，调用方可能收到 nil error 而实际上有分支失败了。
	// 解决方案：等全部结果到齐后，再过滤出第一个非取消的真实错误。
	branches := make([]S, len(targets))
	var firstErr error
	var anyErr error
	for r := range resultCh {
		if r.err != nil {
			if anyErr == nil {
				anyErr = r.err
			}
			// 过滤掉因取消产生的次生错误，只记录第一个真实错误
			if !errors.Is(r.err, context.Canceled) && firstErr == nil {
				firstErr = r.err
			}
		} else {
			branches[r.idx] = r.state
		}
	}
	if firstErr != nil {
		return nil, firstErr
	}
	// 若所有分支报错均为 context.Canceled（例如外部父 Context 被主动取消），不能吞掉错误
	if anyErr != nil {
		return nil, anyErr
	}

	return branches, nil
}
