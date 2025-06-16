package implementation

import (
	"fmt"
	"runtime/debug"
	"github.com/Kush/Database-internals/DataStructures/B-trees/node"
	"github.com/Kush/Database-internals/DataStructures/aggregates/common"
	"github.com/Kush/Database-internals/diskStorage/pagination"
	"github.com/Kush/Database-internals/lib"
)

func (b *BPlusTree) Insert(primaryKey string, value common.Value) lib.Error {

	defer func() {
		if r := recover(); r != nil {
			lib.EmptyError().AddErr(lib.PanicFound, fmt.Errorf("panic recovered: %+v , stack : %v", r, string(debug.Stack())))
		}
	}()

	if b.root == 0 {
		return lib.EmptyError().AddErr(lib.InvalidInputError, fmt.Errorf("insert: tree is empty, cannot insert key %s", primaryKey))
	}
	leafPageID, err := b.findLeafNode(primaryKey)
	if err.IsNotEmpty() {
		return err
	}

	leaf, err2 := b.LoadTreeNode(leafPageID)
	if err2.IsNotEmpty() {
		return err2
	}

	if !leaf.IsFull() {
		return b.insertIntoLeaf(leaf, primaryKey, value)
	}

	leaf, newLeaf, middleNode, err3 := b.splitLeafAndInsert(leaf, primaryKey, value)
	if err3.IsNotEmpty() {
		return err3
	}

	return b.insertIntoParent(leaf, newLeaf, middleNode)
}

func (b *BPlusTree) findLeafNode(primaryKey string) (pagination.PageID, lib.Error) {
	if b.root == 0 {
		return 0, lib.EmptyError().AddErr(lib.InvalidInputError, fmt.Errorf("findLeafNode: tree is empty"))
	}
	currentPage := b.root

	for {
		treeNode, err := b.LoadTreeNode(currentPage)
		if err.IsNotEmpty() {
			return 0, err
		}
		if treeNode == nil {
			return 0, lib.EmptyError()
		}

		if treeNode.IsLeaf() {
			return currentPage, lib.EmptyError()
		}

		childIdx := treeNode.NodesCount()
		for idx, n := range treeNode.Nodes() {
			if primaryKey < n.PrimaryKey() {
				childIdx = idx
				break
			}
		}

		if childIdx >= len(treeNode.ChildTreeNodes()) {
			return 0, lib.EmptyError().AddErr(lib.SystemError, fmt.Errorf("findLeafNode: childIdx %d out of bounds for primary key %s", childIdx, primaryKey))
		}

		currentPage = treeNode.ChildTreeNodes()[childIdx]
	}
}

func (b *BPlusTree) insertIntoLeaf(leaf *node.TreeNode, key string, value common.Value) lib.Error {
	if leaf == nil {
		return lib.EmptyError().AddErr(lib.InvalidInputError, fmt.Errorf("insertIntoLeaf: leaf is nil"))
	}

	err := leaf.InsertInOrder(node.NewNode(key, value))
	if err.IsNotEmpty() {
		return err
	}

	// Mark as dirty and persist to disk
	return b.SaveNode(leaf)
}

func (b *BPlusTree) splitLeafAndInsert(leaf *node.TreeNode, key string, value common.Value) (*node.TreeNode, *node.TreeNode, node.Node, lib.Error) {
	if !leaf.IsLeaf() {
		return nil, nil, node.EmptyNode(), lib.EmptyError().AddErr(lib.InvalidInputError, fmt.Errorf("splitLeafAndInsert: not a leaf node"))
	}


	leaf.InsertInOrder(node.NewNode(key, value))

	left, right, middleNode, err := node.SplitTreeNode(leaf)
	if err.IsNotEmpty() {
		return nil, nil, node.EmptyNode(), err
	}

	newPageID, err2 := b.pager.AllocatePage()
	if err2.IsNotEmpty() {
		return nil, nil, node.EmptyNode(), err2
	}
	right.SetPageID(newPageID)
	left.SetPageID(leaf.PageID())
	left.SetParentNode(leaf.ParentNode())
	right.SetParentNode(leaf.ParentNode())

	if err := b.SaveNode(left); err.IsNotEmpty() {
		return nil, nil, node.EmptyNode(), err
	}
	if err := b.SaveNode(right); err.IsNotEmpty() {
		return nil, nil, node.EmptyNode(), err
	}

	// Update leaf links (if doubly-linked list structure is used)
	// Optional: leaf.SetNext(right.PageID())

	return left, right, middleNode, lib.EmptyError()
}

func (b *BPlusTree) insertIntoParent(leftNode, rightNode *node.TreeNode, middleNode node.Node) lib.Error {
	if leftNode.ParentNode() == 0 {
		rootPageID, err := b.pager.AllocatePage()
		if err.IsNotEmpty() {
			return err
		}
		// Special case: split happened at root → create new root
		newRoot := node.NewInternalNode(rootPageID)
		err = newRoot.InsertInternalKey(middleNode, leftNode.PageID(), rightNode.PageID())
		if err.IsNotEmpty() {
			return err
		}


		leftNode.SetParentNode(rootPageID)
		rightNode.SetParentNode(rootPageID)

		// Persist
		if err := b.SaveNode(newRoot); err.IsNotEmpty() {
			return err
		}
		if err := b.SaveNode(leftNode); err.IsNotEmpty() {
			return err
		}
		if err := b.SaveNode(rightNode); err.IsNotEmpty() {
			return err
		}

		// Update root pointer
		b.updateRoot(rootPageID)
		return lib.EmptyError()
	}

	// Load parent
	parent, err := b.LoadTreeNode(leftNode.ParentNode())
	if err.IsNotEmpty() {
		return err
	}

	err = parent.InsertInternalKey(middleNode, leftNode.PageID(), rightNode.PageID())
	if err.IsNotEmpty() {
		return err
	}

	if !parent.IsFull() {
		return b.SaveNode(parent)
	}

	// Parent split
	newleft, newRight, promotedKey, err := node.SplitTreeNode(parent)
	if err.IsNotEmpty() {
		return err
	}
	newPageID, err2 := b.pager.AllocatePage()
	if err2.IsNotEmpty() {
		return err2
	}
	newRight.SetPageID(newPageID)
	newleft.SetPageID(parent.PageID())
	newRight.SetParentNode(parent.ParentNode())
	newleft.SetParentNode(parent.ParentNode())

	if err := b.SaveNode(newleft); err.IsNotEmpty() {
		return err
	}
	if err := b.SaveNode(newRight); err.IsNotEmpty() {
		return err
	}

	// Recursive promotion
	return b.insertIntoParent(newleft, newRight, promotedKey)
}
