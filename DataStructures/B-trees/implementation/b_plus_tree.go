package implementation

import (
	"github.com/Kush/Database-internals/DataStructures/B-trees/node"
)

type BPlusTree struct {
	root *node.TreeNode
}

func NewBPlusTree() *BPlusTree {
	return &BPlusTree{
		root: nil,
	}
}