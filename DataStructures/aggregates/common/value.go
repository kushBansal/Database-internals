package common

type Value struct {
	stringValue string // maximum 8 bytes
	boolValue   bool
	floatValue  float32
	intValue    int64
}

func EmptyValue() Value {
	return Value{}
}

func NewBoolValue(boolValue bool) Value {
	return NewValue("", boolValue, 0, 0)
}

func NewIntValue(intValue int64) Value {
	return NewValue("", false, 0, intValue)
}

func NewFloatValue(floatValue float32) Value {
	return NewValue("", false, floatValue, 0)
}

func NewStringValue(stringValue string) Value {
	return NewValue(stringValue, false, 0, 0)
}


func NewValue(
	stringValue string,
	boolValue bool,
	floatValue float32,
	intValue int64,
) Value {
	return Value{
		stringValue: stringValue,
		boolValue:   boolValue,
		floatValue:  floatValue,
		intValue:    intValue,
	}
}


func (c Value) StringValue() string {
	return c.stringValue
}

func (c Value) BoolValue() bool {
	return c.boolValue
}

func (c Value) FloatValue() float32 {
	return c.floatValue
}

func (c Value) IntValue() int64 {
	return c.intValue
}


func (c Value) IsEmpty() bool {
	return c.stringValue == "" &&
		!c.boolValue &&
		c.floatValue == 0 &&
		c.intValue == 0
}

