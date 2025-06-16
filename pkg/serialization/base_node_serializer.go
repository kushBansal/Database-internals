package serialization

import "github.com/Kush/Database-internals/lib"

type BaseNodeSerializer[T any] interface {
	Serialize(T) ([]byte, lib.Error)
	Deserialize([]byte, T) lib.Error
}
