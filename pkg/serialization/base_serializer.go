package serialization

import "github.com/Kush/Database-internals/lib"

type BaseSerializer interface {
	Serialize(any) ([]byte, lib.Error)
	Deserialize([]byte, any) lib.Error
}