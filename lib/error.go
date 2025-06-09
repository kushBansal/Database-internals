package lib

import (
	"fmt"
	"runtime"
)

type Error struct {
	errs map[ErrorCode]error
}

func EmptyError() Error {
	return Error{errs: make(map[ErrorCode]error)}
}

func (c Error) Errors() map[ErrorCode]error {
	return c.errs
}

func (c Error) ErrorCodeStrings() []string {
	var errorCodes []string
	for code := range c.errs {
		errorCodes = append(errorCodes, code.ToString())
	}
	return errorCodes
}

func (c Error) ErrorCodes() []ErrorCode {
	var errorCodes []ErrorCode
	for code := range c.errs {
		errorCodes = append(errorCodes, code)
	}
	return errorCodes
}

func (c Error) IsEmpty() bool {
	return len(c.errs) == 0
}

func (c Error) IsNotEmpty() bool {
	return !c.IsEmpty()
}

func (c Error) GetSingleErrorCodeString() string {
	if len(c.errs) == 0 {
		return ""
	}
	for code := range c.errs {
		return code.ToString()
	}
	return ""
}

func (c Error) ContainsError(needle ErrorCode) bool {
	for errorCode := range c.errs {
		if errorCode == needle {
			return true
		}
	}
	return false
}

func (c Error) AddErr(code ErrorCode, err error) Error {
	if c.errs == nil {
		c.errs = make(map[ErrorCode]error)
	}
	c.errs[code] = err
	_, file, line, _ := runtime.Caller(1)
	fmt.Printf("Error added: %s at %s:%d\n", code.ToString(), file, line)
	return c
}

func (c Error) AddWarning(code ErrorCode, err error) Error {
	_, file, line, _ := runtime.Caller(1)
	fmt.Printf("Warning recorded: %s at %s:%d\n", code.ToString(), file, line)
	return c
}
