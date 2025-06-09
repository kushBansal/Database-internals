package node

import (
	"sort"
)

type TreeNode struct {
	nodes          []Node
	childTreeNodes []*TreeNode
	parentNode     *TreeNode
	next           *TreeNode
	leaf           bool
}

func EmptyTreeNode() *TreeNode {
	return &TreeNode{
		nodes:          make([]Node, 0),
		childTreeNodes: make([]*TreeNode, 0),
		parentNode:     nil,
		next:           nil,
		leaf:           false,
	}
}

func NewLeafTreeNode() *TreeNode {
	return &TreeNode{
		nodes:          make([]Node, 0),
		childTreeNodes: make([]*TreeNode, 0),
		parentNode:     nil,
		next:           nil,
		leaf:           true,
	}
}

func (tn *TreeNode) IsLeaf() bool {
	return tn.leaf
}

func (tn *TreeNode) SetLeaf(leaf bool) {
	tn.leaf = leaf
}
func (tn *TreeNode) SetParentNode(parent *TreeNode) {
	tn.parentNode = parent
}

func (tn *TreeNode) ParentNode() *TreeNode {
	return tn.parentNode
}

func (tn *TreeNode) AddChildTreeNode(child *TreeNode) {
	tn.childTreeNodes = append(tn.childTreeNodes, child)
	child.SetParentNode(tn)
}

func (tn *TreeNode) SetChildTreeNodes(childTreeNodes []*TreeNode) {
	tn.childTreeNodes = childTreeNodes
	for _, child := range childTreeNodes {
		child.SetParentNode(tn)
	}
}

func (tn *TreeNode) ChildTreeNodes() []*TreeNode {
	return tn.childTreeNodes
}

func (tn *TreeNode) AddNode(node Node) {
	tn.nodes = append(tn.nodes, node)
}

func (tn *TreeNode) SetNodes(nodes []Node) {
	tn.nodes = nodes
}

func (tn *TreeNode) AddNodes(nodes ...Node) {
	tn.nodes = append(tn.nodes, nodes...)
}

func (tn *TreeNode) NodesCount() int {
	return len(tn.nodes)
}

func (tn *TreeNode) Nodes() []Node {
	return tn.nodes
}

func (tn *TreeNode) SetNext(next *TreeNode) {
	tn.next = next
}

func (tn *TreeNode) Next() *TreeNode {
	return tn.next
}

func (tn *TreeNode) SortNodes() {
	if tn == nil || len(tn.Nodes()) == 0 {
		return
	}

	sort.Slice(tn.Nodes(), func(i, j int) bool {
		return tn.Nodes()[i].PrimaryKey() < tn.Nodes()[j].PrimaryKey()
	})
}
