package datastructures

import (
	"github.com/Kush/Database-internals/DataStructures/aggregates/common"
	"github.com/Kush/Database-internals/lib"
)

type BaseDatabaseStructure interface {
	Insert(primaryKey string, value common.Value) lib.Error
	Search(primaryKey string) (common.Value, lib.Error)
	Delete(primaryKey string) lib.Error
}
