package GopherGraph

import (
	"fmt"
	"slices"
	"strings"
)

// mermaidReservedKeywords 包含 Mermaid flowchart 语法中的保留字
// 若用户的节点名称恰好命中保留字（例如常见的 "end"）直接使用会导致 Mermaid 渲染语法错误
// 因此在生成节点 ID 时会自动添加前缀转义，同时保留原始显示名称
var mermaidReservedKeywords = map[string]bool{
	"end":       true,
	"subgraph":  true,
	"graph":     true,
	"flowchart": true,
	"style":     true,
	"classdef":  true,
	"class":     true,
	"click":     true,
	"default":   true,
	"linkstyle": true,
	"direction": true,
}

// mermaidNodeID 将任意节点名称转换为合法的 Mermaid 节点标识符
func mermaidNodeID(name string) string {
	if mermaidReservedKeywords[strings.ToLower(name)] {
		return "node_" + name
	}
	var sb strings.Builder
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			sb.WriteRune(r)
		} else {
			sb.WriteRune('_')
		}
	}
	id := sb.String()
	if id == "" || (id[0] >= '0' && id[0] <= '9') {
		return "node_" + id
	}
	return id
}

// mermaidEscapeLabel 转义标签中的双引号，防止破坏 Mermaid 节点文本语法
func mermaidEscapeLabel(label string) string {
	return strings.ReplaceAll(label, `"`, `#quot;`)
}

// Mermaid 将编译后的图拓扑结构导出为 Mermaid Flowchart (flowchart TD) 格式字符串。
//
// 视觉规范：
//   - 普通节点：矩形框 node["node"]
//   - 中断节点 (Interrupt)：圆角体育场框 + ⏸ 标识 + 橙色高亮样式
//   - 静态边 (AddEdge)：实线箭头 A --> B
//   - 条件边 (AddConditionalEdgesWithTargets)：虚线箭头 A -.->|conditional| B
//   - 未声明目标的条件边 (AddConditionalEdges)：虚线指向动态路由占位符 A -.->|conditional| A_router{"?"}
//   - 并发边 (AddParallelEdges)：粗实线分流 A ==>|parallel| B 与合并汇聚 B ==>|merge| C
//
// 所有节点和边均按字典序排序后输出，保证多次调用生成的文本完全确定（Deterministic）。
func (cg *CompiledGraph[S]) Mermaid() string {
	var b strings.Builder
	b.WriteString("flowchart TD\n")
	// 1. 收集并排序所有节点名称
	nodeNames := make([]string, 0, len(cg.nodes))
	for name := range cg.nodes {
		nodeNames = append(nodeNames, name)
	}
	slices.Sort(nodeNames)

	// 2. 输出节点定义
	var interruptIDs []string
	for _, name := range nodeNames {
		id := mermaidNodeID(name)
		label := mermaidEscapeLabel(name)
		if cg.interrupts[name] {
			fmt.Fprintf(&b, "    %s([\"⏸ %s (Interrupt)\"])\n", id, label)
			interruptIDs = append(interruptIDs, id)
		} else {
			fmt.Fprintf(&b, "    %s[\"%s\"]\n", id, label)
		}
	}

	// 3. 输出静态边 (AddEdge)
	edgeFroms := make([]string, 0, len(cg.edges))
	for from := range cg.edges {
		edgeFroms = append(edgeFroms, from)
	}
	slices.Sort(edgeFroms)
	for _, from := range edgeFroms {
		to := cg.edges[from]
		fmt.Fprintf(&b, "    %s --> %s\n", mermaidNodeID(from), mermaidNodeID(to))
	}

	// 4. 输出条件边 (AddConditionalEdges / AddConditionalEdgesWithTargets)
	condFroms := make([]string, 0, len(cg.conditional))
	for from := range cg.conditional {
		condFroms = append(condFroms, from)
	}
	slices.Sort(condFroms)
	hasEndNode := false
	for _, from := range condFroms {
		fromID := mermaidNodeID(from)
		if targets, ok := cg.conditionalTargets[from]; ok && len(targets) > 0 {
			for _, target := range targets {
				if target == "" {
					hasEndNode = true
					fmt.Fprintf(&b, "    %s -.->|conditional| __END__([\"🏁 END\"])\n", fromID)
				} else {
					fmt.Fprintf(&b, "    %s -.->|conditional| %s\n", fromID, mermaidNodeID(target))
				}
			}
		} else {
			routerID := fromID + "_router"
			fmt.Fprintf(&b, "    %s -.->|conditional| %s{\"?\"}\n", fromID, routerID)
		}
	}

	// 5. 输出并发边 (AddParallelEdges)
	parallelFroms := make([]string, 0, len(cg.parallels))
	for from := range cg.parallels {
		parallelFroms = append(parallelFroms, from)
	}
	slices.Sort(parallelFroms)
	for _, from := range parallelFroms {
		step := cg.parallels[from]
		fromID := mermaidNodeID(from)
		for _, target := range step.targets {
			targetID := mermaidNodeID(target)
			fmt.Fprintf(&b, "    %s ==>|parallel| %s\n", fromID, targetID)
			if step.next != "" {
				fmt.Fprintf(&b, "    %s ==>|merge| %s\n", targetID, mermaidNodeID(step.next))
			}
		}
	}

	// 6. 为中断节点和结束节点添加高亮样式
	for _, id := range interruptIDs {
		fmt.Fprintf(&b, "    style %s fill:#fff3cd,stroke:#ff9800,stroke-width:2px,color:#000\n", id)
	}
	if hasEndNode {
		b.WriteString("    style __END__ fill:#e8f5e9,stroke:#4caf50,stroke-width:2px,color:#000\n")
	}
	return b.String()
}

// Mermaid 导出底层已编译图的 Mermaid 流程图字符串。
func (e *Engine[S]) Mermaid() string {
	return e.graph.Mermaid()
}
