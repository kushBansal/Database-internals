package implementation

import (
	"fmt"

	"github.com/Kush/Database-internals/lib"
	"github.com/Kush/Database-internals/DataStructures/B-trees/node"
	"github.com/Kush/Database-internals/DataStructures/aggregates/common"
)

func (b *BPlusTree) Search(primaryKey string) (common.Value, lib.Error) {
	if b== nil {
		return common.Value{}, lib.EmptyError()
	}

	return search(b.root, primaryKey)
}

func search(treeNode *node.TreeNode, primaryKey string) (common.Value, lib.Error) {
	if treeNode == nil {
		return common.Value{}, lib.EmptyError()
	}

	childIdx := treeNode.NodesCount()
	for idx, node := range treeNode.Nodes() {
		if primaryKey < node.PrimaryKey() {
			childIdx = idx
			break
		}
	}

	if treeNode.IsLeaf() {
		if childIdx > 0 {
			if treeNode.Nodes()[childIdx-1].PrimaryKey() == primaryKey {
				return treeNode.Nodes()[childIdx-1].Value(), lib.EmptyError()
			}
		}
		return common.Value{}, lib.EmptyError()
	}

	if childIdx < len(treeNode.ChildTreeNodes()) {
		return search(treeNode.ChildTreeNodes()[childIdx], primaryKey)
	}

	return common.Value{}, lib.EmptyError().AddErr(lib.SystemError, fmt.Errorf("search: primary key %s not found", primaryKey))
}

