package serializer

import (
	"fmt"

	"github.com/Kush/Database-internals/DataStructures/B-trees/constants"
	data_node "github.com/Kush/Database-internals/DataStructures/B-trees/node"
	"github.com/Kush/Database-internals/DataStructures/aggregates/common"
	"github.com/Kush/Database-internals/diskStorage/pagination"
	"github.com/Kush/Database-internals/lib"
	"github.com/Kush/Database-internals/pkg/serialization"
)

type TreeNodeSerializer[T any] struct {
	serializer serialization.BaseSerializer
}

func NewTreeNodeSerializer[T any](serializer serialization.BaseSerializer) *TreeNodeSerializer[T] {
	return &TreeNodeSerializer[T]{
		serializer: serializer,
	}
}

func (tn *TreeNodeSerializer[T]) Serialize(val T) ([]byte, lib.Error) {
	node, ok := any(val).(*data_node.TreeNode)
	if !ok {
		return nil, lib.EmptyError().AddErr(lib.SerializationError, fmt.Errorf("val must implement proto.Message"))
	}

	buf := make([]byte, 0, constants.PageSize)

	var nodePageID uint32 = uint32(node.PageID()) // you can replace with actual ID
	b, err := tn.serializer.Serialize(nodePageID)
	if err.IsNotEmpty() {
		return nil, err
	}
	buf = append(buf, b...)

	// Leaf flag
	var leafByte uint8
	if node.IsLeaf() {
		leafByte = 1
	}
	buf = append(buf, leafByte)

	// Number of nodes
	numNodes := uint16(len(node.Nodes()))
	b, err = tn.serializer.Serialize(numNodes)
	if err.IsNotEmpty() {
		return nil, err
	}
	buf = append(buf, b...)

	// Next pointer (placeholder page ID, use 0 if nil)
	var nextPageID uint32 = uint32(node.Next()) // you can replace with actual ID
	b, err = tn.serializer.Serialize(nextPageID)
	if err.IsNotEmpty() {
		return nil, err
	}
	buf = append(buf, b...)

	var parentPageID uint32 = uint32(node.ParentNode()) // you can replace with actual ID
	b, err = tn.serializer.Serialize(parentPageID)
	if err.IsNotEmpty() {
		return nil, err
	}
	buf = append(buf, b...)

	// Serialize each node
	for _, n := range node.Nodes() {
		// Key
		b, err := tn.serializer.Serialize(n.PrimaryKey())
		if err.IsNotEmpty() {
			return nil, err
		}
		buf = append(buf, b...)

		// Value
		v := n.Value()

		// stringValue
		strVal, err := tn.serializer.Serialize(v.StringValue())
		if err.IsNotEmpty() {
			return nil, err
		}
		buf = append(buf, strVal...)

		// boolValue
		if v.BoolValue() {
			buf = append(buf, byte(1))
		} else {
			buf = append(buf, byte(0))
		}

		// float32
		b, err = tn.serializer.Serialize(v.FloatValue())
		if err.IsNotEmpty() {
			return nil, err
		}
		buf = append(buf, b...)

		// int64
		b, err = tn.serializer.Serialize(v.IntValue())
		if err.IsNotEmpty() {
			return nil, err
		}
		buf = append(buf, b...)
	}

	// Serialize child references as page IDs (uint32 placeholders)
	childCount := len(node.ChildTreeNodes())
	for i := 0; i < childCount; i++ {
		childID := uint32(node.ChildTreeNodes()[i])
		b, err = tn.serializer.Serialize(childID)
		if err.IsNotEmpty() {
			return nil, err
		}
		buf = append(buf, b...)
	}

	// Ensure we don’t exceed page size
	if len(buf) > constants.PageSize {
		return nil, lib.EmptyError().AddErr(lib.SerializationError, fmt.Errorf("tree node exceeds 4KB page size"))
	}

	return buf, lib.EmptyError()
}

func (tn *TreeNodeSerializer[T]) Deserialize(data []byte, v T) lib.Error {
	node, ok := any(v).(*data_node.TreeNode)
	if !ok {
		return lib.EmptyError().AddErr(lib.DeserializationError, fmt.Errorf("v must implement *node.TreeNode"))
	}
	offset := 0

	var pageId uint32
	if err := tn.serializer.Deserialize(data[offset:offset+4], &pageId); err.IsNotEmpty() {
		return err
	}
	node.SetPageID(pagination.PageID(pageId))
	offset += 4

	// 1. Leaf flag
	if offset+1 > len(data) {
		return lib.EmptyError().AddErr(lib.DeserializationError, fmt.Errorf("invalid data: missing leaf flag"))
	}
	leafByte := data[offset]
	node.SetLeaf(leafByte == 1)
	offset += 1

	// 2. Number of nodes
	numNodesRaw := data[offset : offset+2]
	var numNodes uint16
	if err := tn.serializer.Deserialize(numNodesRaw, &numNodes); err.IsNotEmpty() {
		return err
	}
	offset += 2

	// 3. Next Page ID
	var nextID uint32
	if err := tn.serializer.Deserialize(data[offset:offset+4], &nextID); err.IsNotEmpty() {
		return err
	}
	node.SetNext(pagination.PageID(nextID))
	offset += 4

	// 4. Parent Page ID
	var parentID uint32
	if err := tn.serializer.Deserialize(data[offset:offset+4], &parentID); err.IsNotEmpty() {
		return err
	}
	node.SetParentNode(pagination.PageID(parentID))
	offset += 4

	// 5. Deserialize nodes
	nodes := make([]data_node.Node, 0, numNodes)
	for i := 0; i < int(numNodes); i++ {
		// Primary key (string)
		var key string
		if err := tn.serializer.Deserialize(data[offset:], &key); err.IsNotEmpty() {
			return err
		}
		keyLen := len(key)
		offset += 2 + keyLen // assumes prefix length encoding

		// StringValue (string)
		var strVal string
		if err := tn.serializer.Deserialize(data[offset:], &strVal); err.IsNotEmpty() {
			return err
		}
		strValLen := len(strVal)
		offset += 2 + strValLen

		// BoolValue
		boolVal := data[offset] == 1
		offset += 1

		// Float32
		var floatVal float32
		if err := tn.serializer.Deserialize(data[offset:offset+4], &floatVal); err.IsNotEmpty() {
			return err
		}
		offset += 4

		// Int64
		var intVal int64
		if err := tn.serializer.Deserialize(data[offset:offset+8], &intVal); err.IsNotEmpty() {
			return err
		}
		offset += 8

		val := common.NewValue(strVal, boolVal, floatVal, intVal)
		nodes = append(nodes, data_node.NewNode(key, val))
	}
	node.SetNodes(nodes)

	// 6. Deserialize child page IDs
	childRefs := make([]pagination.PageID, 0, numNodes+1)
	for i := 0; i < int(numNodes)+1; i++ {
		var childID uint32
		if err := tn.serializer.Deserialize(data[offset:offset+4], &childID); err.IsNotEmpty() {
			return err
		}
		childRefs = append(childRefs, pagination.PageID(childID))
		offset += 4
	}
	node.SetChildTreeNodes(childRefs)

	return lib.EmptyError()
}
