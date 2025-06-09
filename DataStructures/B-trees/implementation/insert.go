package implementation

import (
	"fmt"

	"github.com/Kush/Database-internals/DataStructures/B-trees/constants"
	"github.com/Kush/Database-internals/DataStructures/B-trees/node"
	"github.com/Kush/Database-internals/DataStructures/aggregates/common"
	"github.com/Kush/Database-internals/lib"
)

func (b *BPlusTree) Insert(primaryKey string, value common.Value) lib.Error {
	if b == nil {
		return lib.EmptyError().AddErr(lib.InvalidInputError, fmt.Errorf("insert: BPlusTree is nil"))
	}

	if b.root == nil {
		b.root = node.NewLeafTreeNode()
	}

	rootNode, err := insert(b.root, primaryKey, value)
	if err.IsNotEmpty() {
		return err
	}

	b.root = rootNode
	return lib.EmptyError()
}

func insert(treeNode *node.TreeNode, primaryKey string, value common.Value) (*node.TreeNode, lib.Error) {
	if treeNode == nil {
		return nil, lib.EmptyError().AddErr(lib.InvalidInputError, fmt.Errorf("insert: treeNode is nil"))
	}

	if treeNode.IsLeaf() {
		for idx, existingNode := range treeNode.Nodes() {
			if existingNode.PrimaryKey() == primaryKey {
				treeNode.Nodes()[idx] = node.NewNode(primaryKey, value)
				return treeNode, lib.EmptyError()
			}
		}

		newNode := node.NewNode(primaryKey, value)
		treeNode.AddNode(newNode)
		treeNode.SortNodes()
		if len(treeNode.Nodes()) <= constants.MaxNodesInTreeNode {
			return treeNode, lib.EmptyError()
		}

		numOfNodesInRightsubtree := (len(treeNode.Nodes()) + 1) / 2
		nodeTobeAppendinParent := treeNode.Nodes()[(constants.MaxNodesInTreeNode + 1 - numOfNodesInRightsubtree)]
		newTreenode := node.NewLeafTreeNode()
		newTreenode.AddNodes(treeNode.Nodes()[(constants.MaxNodesInTreeNode + 1 - numOfNodesInRightsubtree):]...)
		treeNode.SetNodes(treeNode.Nodes()[:(constants.MaxNodesInTreeNode + 1 - numOfNodesInRightsubtree)])
		parentNode := treeNode.ParentNode()
		if parentNode != nil {
			parentNode.AddChildTreeNode(newTreenode)
			parentNode.AddNode(nodeTobeAppendinParent)
			parentNode.SortNodes()
			return treeNode, lib.EmptyError()
		}

		newRootnode := node.EmptyTreeNode()
		newRootnode.AddNode(nodeTobeAppendinParent)
		newRootnode.AddChildTreeNode(treeNode)
		newRootnode.AddChildTreeNode(newTreenode)
		newRootnode.SortNodes()

		return newRootnode, lib.EmptyError()
	}

	childIdx := treeNode.NodesCount()
	for idx, childNode := range treeNode.Nodes() {
		if primaryKey < childNode.PrimaryKey() {
			childIdx = idx
			break
		}
	}

	if childIdx >= len(treeNode.ChildTreeNodes()) {
		return nil, lib.EmptyError().AddErr(lib.SystemError, fmt.Errorf("insert: treeNode is not a leaf and no child tree node found for primary key %s", primaryKey))
	}

	childTreeNode := treeNode.ChildTreeNodes()[childIdx]
	if childTreeNode == nil {
		return nil, lib.EmptyError().AddErr(lib.SystemError, fmt.Errorf("insert: child tree node is nil at index %d for primary key %s", childIdx, primaryKey))
	}

	_, err := insert(childTreeNode, primaryKey, value)
	if err.IsNotEmpty() {
		return nil, err
	}

	treeNode.SortNodes()
	if len(treeNode.Nodes()) <= constants.MaxNodesInTreeNode {
		return treeNode, lib.EmptyError()
	}

	numOfNodesInRightsubtree := (len(treeNode.Nodes()) + 1) / 2
	nodeTobeAppendinParent := treeNode.Nodes()[(constants.MaxNodesInTreeNode + 1 - numOfNodesInRightsubtree)]
	newTreenode := node.EmptyTreeNode()
	newTreenode.AddNodes(treeNode.Nodes()[(constants.MaxNodesInTreeNode + 2 - numOfNodesInRightsubtree):]...)
	treeNode.SetNodes(treeNode.Nodes()[:(constants.MaxNodesInTreeNode + 1 - numOfNodesInRightsubtree)])
	newTreenode.SetChildTreeNodes(treeNode.ChildTreeNodes()[(constants.MaxNodesInTreeNode/2+1):])
	treeNode.SetChildTreeNodes(treeNode.ChildTreeNodes()[:(constants.MaxNodesInTreeNode/2+1)])
	parentNode := treeNode.ParentNode()
	if parentNode != nil {
		parentNode.AddChildTreeNode(newTreenode)
		parentNode.AddNode(nodeTobeAppendinParent)
		parentNode.SortNodes()
		return treeNode, lib.EmptyError()
	}

	newRootnode := node.EmptyTreeNode()
	newRootnode.AddNode(nodeTobeAppendinParent)
	newRootnode.AddChildTreeNode(treeNode)
	newRootnode.AddChildTreeNode(newTreenode)
	newRootnode.SortNodes()

	return newRootnode, lib.EmptyError()
}
