package node

import (
	"fmt"
	"sort"

	"github.com/Kush/Database-internals/DataStructures/B-trees/constants"
	"github.com/Kush/Database-internals/diskStorage/pagination"
	"github.com/Kush/Database-internals/lib"
)

type TreeNode struct {
	pageId         pagination.PageID
	nodes          []Node
	childTreeNodes []pagination.PageID
	parentNode     pagination.PageID
	next           pagination.PageID
	leaf           bool
}

func NewInternalNode(pageId pagination.PageID) *TreeNode {
	return &TreeNode{
		pageId:         pageId,
		nodes:          make([]Node, 0),
		childTreeNodes: make([]pagination.PageID, 0),
		leaf:           false,
	}
}

func NewLeafTreeNode(pageId pagination.PageID) *TreeNode {
	return &TreeNode{
		pageId:         pageId,
		nodes:          make([]Node, 0),
		childTreeNodes: make([]pagination.PageID, 0),
		leaf:           true,
	}
}

func NewTreeNode() *TreeNode {
	return &TreeNode{
		nodes:          make([]Node, 0),
		childTreeNodes: make([]pagination.PageID, 0),
		leaf:           false,
	}
}

func (tn *TreeNode) PageID() pagination.PageID {
	return tn.pageId
}

func (tn *TreeNode) IsLeaf() bool {
	return tn.leaf
}

func (tn *TreeNode) IsFull() bool {
	return (len(tn.nodes) >= constants.MaxNodesInTreeNode)
}

func (tn *TreeNode) IsValidNumberOfNodes() bool {
	return len(tn.nodes) <= constants.MaxNodesInTreeNode
}

func (tn *TreeNode) SetLeaf(leaf bool) {
	tn.leaf = leaf
}
func (tn *TreeNode) SetParentNode(parent pagination.PageID) {
	tn.parentNode = parent
}

func (tn *TreeNode) ParentNode() pagination.PageID {
	return tn.parentNode
}

func (tn *TreeNode) AddChildTreeNode(child pagination.PageID) {
	tn.childTreeNodes = append(tn.childTreeNodes, child)
}

func (tn *TreeNode) SetChildTreeNodes(childTreeNodes []pagination.PageID) {
	tn.childTreeNodes = childTreeNodes
}

func (tn *TreeNode) ChildTreeNodes() []pagination.PageID {
	return tn.childTreeNodes
}

func (tn *TreeNode) SetPageID(pageID pagination.PageID) {
	tn.pageId = pageID
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

func (tn *TreeNode) SetNext(next pagination.PageID) {
	tn.next = next
}

func (tn *TreeNode) Next() pagination.PageID {
	return tn.next
}

func (tn *TreeNode) IsPrimaryKeyfound(key string) bool {
	for _, node := range tn.Nodes() {
		if node.PrimaryKey() == key {
			return true
		}
	}

	return false
}

func (tn *TreeNode) SortNodes() {
	if tn == nil || len(tn.Nodes()) == 0 {
		return
	}

	sort.Slice(tn.Nodes(), func(i, j int) bool {
		return tn.Nodes()[i].PrimaryKey() < tn.Nodes()[j].PrimaryKey()
	})
}

func SplitTreeNode(n *TreeNode) (*TreeNode, *TreeNode, Node, lib.Error) {
	if n.IsLeaf() {
		mid := len(n.Nodes()) / 2
		leftTreeNode := NewTreeNode()
		rightTreeNode := NewTreeNode()
		leftTreeNode.SetLeaf(true)
		rightTreeNode.SetLeaf(true)
		leftTreeNode.SetPageID(n.PageID())
		leftTreeNode.SetNodes(n.Nodes()[:mid])
		rightTreeNode.SetNodes(n.Nodes()[mid:])
		leftTreeNode.SetParentNode(n.ParentNode())
		rightTreeNode.SetParentNode(n.ParentNode())

		middleNode := rightTreeNode.Nodes()[0]
		return leftTreeNode, rightTreeNode, middleNode, lib.EmptyError()
	}

	mid := len(n.Nodes()) / 2
	leftTreeNode := NewTreeNode()
	rightTreeNode := NewTreeNode()
	leftTreeNode.SetPageID(n.PageID())
	leftTreeNode.SetNodes(n.Nodes()[:mid])
	rightTreeNode.SetNodes(n.Nodes()[mid+1:])
	leftTreeNode.SetChildTreeNodes(n.ChildTreeNodes()[:mid+1])
	rightTreeNode.SetChildTreeNodes(n.ChildTreeNodes()[mid+1:])
	leftTreeNode.SetParentNode(n.ParentNode())
	rightTreeNode.SetParentNode(n.ParentNode())

	middleNode := n.Nodes()[mid]
	return leftTreeNode, rightTreeNode, middleNode, lib.EmptyError()
}

func (n *TreeNode) InsertInternalKey(node Node, leftPageID, rightPageID pagination.PageID) lib.Error {
	if n.IsLeaf() {
		return lib.EmptyError().AddErr(lib.SystemError, fmt.Errorf("InsertInternalKey: called on leaf node"))
	}

	insertIdx := len(n.Nodes())
	for i, entry := range n.Nodes() {
		if node.primaryKey < entry.PrimaryKey() {
			insertIdx = i
			break
		}
	}

	n.nodes = append(n.nodes, EmptyNode())
	copy(n.nodes[insertIdx+1:], n.nodes[insertIdx:])
	n.nodes[insertIdx] = node

	if len(n.childTreeNodes) == 0 {
		n.childTreeNodes = append(n.childTreeNodes, leftPageID, rightPageID)
		return lib.EmptyError()
	}
	n.childTreeNodes = append(n.childTreeNodes, 0)
	copy(n.childTreeNodes[insertIdx+2:], n.childTreeNodes[insertIdx+1:])
	n.childTreeNodes[insertIdx+1] = rightPageID

	return lib.EmptyError()
}

func (n *TreeNode) InsertInOrder(node Node) lib.Error {
	insertIdx := len(n.Nodes())
	for i, entry := range n.Nodes() {
		if entry.PrimaryKey() == node.primaryKey {
			n.nodes[i].value = node.Value()
			return lib.EmptyError()
		}
		if node.primaryKey < entry.PrimaryKey() {
			insertIdx = i
			break
		}
	}

	// Insert the key at insertIdx
	n.nodes = append(n.nodes, EmptyNode())
	copy(n.nodes[insertIdx+1:], n.nodes[insertIdx:])
	n.nodes[insertIdx] = node

	return lib.EmptyError()
}
