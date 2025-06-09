package serialization

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/Kush/Database-internals/lib"
)

type BinarySerializer struct {
}

func NewBinarySerializer() *BinarySerializer {
	return &BinarySerializer{}
}

func (s *BinarySerializer) Serialize(val any) ([]byte, lib.Error) {
	switch v := any(val).(type) {
	case uint32:
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, v)
		return buf, lib.EmptyError()
	case int32:
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, uint32(v))
		return buf, lib.EmptyError()
	case float32:
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, math.Float32bits(v))
		return buf, lib.EmptyError()
	case uint64:
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, v)
		return buf, lib.EmptyError()
	case int64:
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, uint64(v))
		return buf, lib.EmptyError()
	case float64:
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, math.Float64bits(v))
		return buf, lib.EmptyError()
	case int16:
		buf := make([]byte, 2)
		binary.LittleEndian.PutUint16(buf, uint16(v))
		return buf, lib.EmptyError()
	case uint16:
		buf := make([]byte, 2)
		binary.LittleEndian.PutUint16(buf, v)
		return buf, lib.EmptyError()
	case string:
		buf := make([]byte, len(v))
		copy(buf, []byte(v))
		return buf, lib.EmptyError()
	}

	return nil, lib.EmptyError().AddErr(lib.SerializationError, fmt.Errorf("unsupported type for serialization"))
}

func (s *BinarySerializer) Deserialize(data []byte, target any) lib.Error {
	if len(data) == 0 {
		return lib.EmptyError().AddErr(lib.DeserializationError, fmt.Errorf("input data is empty"))
	}

	switch v := target.(type) {
	case *uint32:
		if len(data) != 4 {
			return lib.EmptyError().AddErr(lib.InvalidByteLength, fmt.Errorf("expected 4 bytes for uint32, got %d", len(data)))
		}
		*v = binary.LittleEndian.Uint32(data)
	case *int32:
		if len(data) != 4 {
			return lib.EmptyError().AddErr(lib.InvalidByteLength, fmt.Errorf("expected 4 bytes for int32, got %d", len(data)))
		}
		*v = int32(binary.LittleEndian.Uint32(data))
	case *float32:
		if len(data) != 4 {
			return lib.EmptyError().AddErr(lib.InvalidByteLength, fmt.Errorf("expected 4 bytes for float32, got %d", len(data)))
		}
		*v = math.Float32frombits(binary.LittleEndian.Uint32(data))
	case *uint64:
		if len(data) != 8 {
			return lib.EmptyError().AddErr(lib.InvalidByteLength, fmt.Errorf("expected 8 bytes for uint64, got %d", len(data)))
		}
		*v = binary.LittleEndian.Uint64(data)
	case *int64:
		if len(data) != 8 {
			return lib.EmptyError().AddErr(lib.InvalidByteLength, fmt.Errorf("expected 8 bytes for int64, got %d", len(data)))
		}
		*v = int64(binary.LittleEndian.Uint64(data))
	case *float64:
		if len(data) != 8 {
			return lib.EmptyError().AddErr(lib.InvalidByteLength, fmt.Errorf("expected 8 bytes for float64, got %d", len(data)))
		}
		*v = math.Float64frombits(binary.LittleEndian.Uint64(data))
	case *int16:
		if len(data) != 2 {
			return lib.EmptyError().AddErr(lib.InvalidByteLength, fmt.Errorf("expected 2 bytes for int16, got %d", len(data)))
		}
		*v = int16(binary.LittleEndian.Uint16(data))
	case *uint16:
		if len(data) != 2 {
			return lib.EmptyError().AddErr(lib.InvalidByteLength, fmt.Errorf("expected 2 bytes for uint16, got %d", len(data)))
		}
		*v = binary.LittleEndian.Uint16(data)
	case *string:
		// For strings, your Serialize function just copies the bytes.
		// So, we assume the entire byte slice is the string.
		*v = string(data)
	default:
		return lib.EmptyError().AddErr(lib.UnsupportedTypeError, fmt.Errorf("unsupported target type for deserialization: %T", target))
	}
	return lib.EmptyError()
}
