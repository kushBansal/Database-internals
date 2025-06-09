package node

import "github.com/Kush/Database-internals/DataStructures/aggregates/common"

type Node struct {
	primaryKey string
	value      common.Value
}

func NewNode(primaryKey string, value common.Value) Node {
	return Node{
		primaryKey: primaryKey,
		value:      value,
	}
}

func (n Node) PrimaryKey() string {
	return n.primaryKey
}

func (n Node) Value() common.Value {
	return n.value
}
