// Package GopherGraph 是一个用 Go 语言编写的代码即图（Code-as-Graph）智能体编排引擎，
// 设计灵感来源于 Python 生态的 LangGraph。
//
// 它利用 Go 1.18+ 的泛型机制，让工作流的上下文状态在编译期就具备强类型约束，
// 并依靠 Go 原生的 Goroutines 和 Channels 实现极高并发的本地数据流转。
//
// # 核心概念
//
//   - [Graph] 图构建器：通过 [NewGraph] 创建，使用 AddNode/AddEdge 系列方法声明节点和连线。
//   - [CompiledGraph] 编译后的只读图：通过 [Graph.Compile] 生成，可直接调用 Start/Resume 运行。
//   - [Engine] 增强包装器：通过 [NewEngine] 创建，在 CompiledGraph 基础上叠加
//     StateCloner（深拷贝）、MaxSteps（死循环熔断）、PreNodeHook/PostNodeHook（生命周期钩子）。
//   - [Thread] 执行快照：承载当前状态、下一节点、暂停/结束标记，可序列化用于断点恢复。
//   - [Checkpointer] 持久化接口：内置 [FileCheckpointer] 实现基于 JSON 文件的状态存储。
//
// # 零外部依赖
//
// GopherGraph 仅使用 Go 标准库，无任何第三方依赖。
package GopherGraph
