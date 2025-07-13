package implementation

import (
	"fmt"
	"runtime/debug"

	"github.com/Kush/Database-internals/DataStructures/aggregates/common"
	"github.com/Kush/Database-internals/diskStorage/pagination"
	"github.com/Kush/Database-internals/lib"
)

func (b *BPlusTree) Search(primaryKey string) (common.Value, lib.Error) {
	defer func() {
		if r := recover(); r != nil {
			lib.EmptyError().AddErr(lib.PanicFound, fmt.Errorf("panic recovered: %+v , stack : %v", r, string(debug.Stack())))
		}
	}()
	if b == nil {
		return common.Value{}, lib.EmptyError()
	}

	return b.search(b.root, primaryKey)
}

func (b *BPlusTree) search(nodePage pagination.PageID, primaryKey string) (common.Value, lib.Error) {
	if nodePage == 0 {
		return common.Value{}, lib.EmptyError().AddErr(lib.InitError, fmt.Errorf("root page Id is not set"))
	}

	treeNode, err := b.LoadTreeNode(nodePage)
	if err.IsNotEmpty() {
		return common.Value{}, err
	}
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
		return b.search(treeNode.ChildTreeNodes()[childIdx], primaryKey)
	}

	return common.Value{}, lib.EmptyError().AddErr(lib.SystemError, fmt.Errorf("search: primary key %s not found", primaryKey))
}
