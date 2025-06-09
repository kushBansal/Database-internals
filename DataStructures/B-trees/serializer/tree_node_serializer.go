package serializer

import (
	"fmt"

	"github.com/Kush/Database-internals/DataStructures/B-trees/constants"
	"github.com/Kush/Database-internals/DataStructures/B-trees/node"
	"github.com/Kush/Database-internals/lib"
	"github.com/Kush/Database-internals/pkg/serialization"
)

type TreeNodeSerializer struct {
	serializer serialization.BaseSerializer
}

func NewTreeNodeSerializer(serializer serialization.BaseSerializer) *TreeNodeSerializer {
	return &TreeNodeSerializer{
		serializer: serializer,
	}
}

func (tn *TreeNodeSerializer) Serialize(node *node.TreeNode) ([]byte, lib.Error) {
	buf := make([]byte, 0, constants.PageSize)

	// Leaf flag
	var leafByte uint8
	if node.IsLeaf() {
		leafByte = 1
	}
	buf = append(buf, leafByte)

	// Number of nodes
	numNodes := uint16(len(node.Nodes()))
	b, err := tn.serializer.Serialize(numNodes)
	if err.IsNotEmpty() {
		return nil, err
	}
	buf = append(buf, b...)

	// Next pointer (placeholder page ID, use 0 if nil)
	var nextPageID uint32 = 0 // you can replace with actual ID
	b, err = tn.serializer.Serialize(nextPageID)
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
	childCount := len(node.Nodes()) + 1
	for i := 0; i < childCount; i++ {
		childID := uint32(0) // Replace with actual child page ID
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
