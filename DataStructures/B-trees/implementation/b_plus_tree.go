package implementation

import (
	"fmt"

	"github.com/Kush/Database-internals/DataStructures/B-trees/node"
	"github.com/Kush/Database-internals/diskStorage/pagination"
	"github.com/Kush/Database-internals/lib"
	"github.com/Kush/Database-internals/pkg/serialization"
)

type BPlusTree struct {
	root             pagination.PageID
	pager            pagination.BasePagination
	serializer       serialization.BaseNodeSerializer[*node.TreeNode]
	binarySerializer serialization.BaseSerializer
}

func NewBPlusTree(
	root pagination.PageID,
	pager pagination.BasePagination,
	serializer serialization.BaseNodeSerializer[*node.TreeNode],
	binarySerializer serialization.BaseSerializer,
) *BPlusTree {
	return &BPlusTree{
		root:             root,
		pager:            pager,
		serializer:       serializer,
		binarySerializer: binarySerializer,
	}
}

func (b *BPlusTree) Init() lib.Error {
	var initPage pagination.PageID = 0
	var err lib.Error
	if b.pager.NumPages() == 0 {
		initPage, err = b.pager.AllocatePage()
		if err.IsNotEmpty() {
			return err
		}
	}
	buf, err := b.pager.ReadPage(initPage)
	if err.IsNotEmpty() {
		return err
	}

	if len(buf) < 4 {
		return lib.EmptyError()
	}

	var rootPageID uint32
	if e := b.binarySerializer.Deserialize(buf[:4], &rootPageID); e.IsNotEmpty() {
		return e
	}

	if rootPageID == 0 {
		newPageID, err := b.pager.AllocatePage()
		if err.IsNotEmpty() {
			return err
		}

		newRoot := node.NewLeafTreeNode(newPageID)
		if err.IsNotEmpty() {
			return err
		}
		rootPageID = uint32(newPageID)
		if err := b.SaveNode(newRoot); err.IsNotEmpty() {
			return err
		}
		err = b.updateRoot(newPageID)
		if err.IsNotEmpty() {
			return err
		}
	}
	b.root = pagination.PageID(rootPageID)

	return lib.EmptyError()
}

func (b *BPlusTree) updateRoot(newRoot pagination.PageID) lib.Error {
	if newRoot == 0 {
		return lib.EmptyError().AddErr(lib.InvalidInputError, fmt.Errorf("updateRoot: newRoot is zero"))
	}
	b.root = newRoot
	buff, err := b.binarySerializer.Serialize(uint32(newRoot))
	if err.IsNotEmpty() {
		return err
	}
	return b.pager.WritePage(0, buff)
}

func (b *BPlusTree) LoadTreeNode(pageID pagination.PageID) (*node.TreeNode, lib.Error) {
	if pageID == 0 {
		return nil, lib.EmptyError().AddErr(lib.InvalidInputError, fmt.Errorf("LoadTreeNode: pageID is zero"))
	}
	buf, err := b.pager.ReadPage(pageID)
	if err.IsNotEmpty() {
		return nil, err
	}

	node := &node.TreeNode{}
	if e := b.serializer.Deserialize(buf, node); e.IsNotEmpty() {
		return nil, e
	}
	return node, lib.EmptyError()
}

func (b *BPlusTree) SaveNode(node *node.TreeNode) lib.Error {
	buf, err := b.serializer.Serialize(node)
	if err.IsNotEmpty() {
		return err
	}
	err = b.pager.WritePage(node.PageID(), buf)
	if err.IsNotEmpty() {
		return err
	}
	return b.pager.Sync()
}
