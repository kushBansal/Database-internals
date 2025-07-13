package lib

type ErrorCode string

const (
	SystemError          ErrorCode = "SystemError"
	InvalidInputError    ErrorCode = "InvalidInputError"
	PaginationError      ErrorCode = "PaginationError"
	SerializationError   ErrorCode = "SerializationError"
	InvalidByteLength    ErrorCode = "InvalidByteLength"
	DeserializationError ErrorCode = "DeserializationError"
	UnsupportedTypeError ErrorCode = "UnsupportedTypeError"
	PanicFound           ErrorCode = "PanicFoundError"
	InitError            ErrorCode = "InitError"
)

func (e ErrorCode) ToString() string {
	return string(e)
}
