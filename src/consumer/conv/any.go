package conv

import "strings"

func AnyToInt64(val any) int64 {
	if val == nil {
		return 0
	}
	var i int64
	switch val.(type) {
	case bool:
		if val.(bool) {
			i = 1
		}
	case int64:
		i = val.(int64)
	case uint64:
		i = int64(val.(uint64))
	case int:
		i = int64(val.(int))
	case uint:
		i = int64(val.(uint))
	case int8:
		i = int64(val.(int8))
	case uint8:
		i = int64(val.(uint8))
	case int16:
		i = int64(val.(int16))
	case uint16:
		i = int64(val.(uint16))
	case int32:
		i = int64(val.(int32))
	case uint32:
		i = int64(val.(uint64))
	case string:
		i = Atoi64(val.(string), 0)
	case []byte:
		i = Atoi64(string(val.([]byte)), 0)
	default:
	}

	return i
}

func AnyToUint64(val any) uint64 {
	if val == nil {
		return 0
	}
	var i uint64
	switch val.(type) {
	case bool:
		if val.(bool) {
			i = 1
		}
	case int64:
		i = uint64(val.(int64))
	case uint64:
		i = val.(uint64)
	case int:
		i = uint64(val.(int))
	case uint:
		i = uint64(val.(uint))
	case int8:
		i = uint64(val.(int8))
	case uint8:
		i = uint64(val.(uint8))
	case int16:
		i = uint64(val.(int16))
	case uint16:
		i = uint64(val.(uint16))
	case int32:
		i = uint64(val.(int32))
	case uint32:
		i = val.(uint64)
	case string:
		i = Atou64(val.(string), 0)
	case []byte:
		i = Atou64(string(val.([]byte)), 0)
	default:
	}

	return i
}

func AnyToFloat64(val any) float64 {
	if val == nil {
		return 0
	}
	var f float64
	switch val.(type) {
	case bool:
		if val.(bool) {
			f = 1
		}
	case int64:
		f = float64(val.(int64))
	case uint64:
		f = float64(val.(uint64))
	case int:
		f = float64(val.(int))
	case uint:
		f = float64(val.(uint))
	case int8:
		f = float64(val.(int8))
	case uint8:
		f = float64(val.(uint8))
	case int16:
		f = float64(val.(int16))
	case uint16:
		f = float64(val.(uint16))
	case int32:
		f = float64(val.(int32))
	case uint32:
		f = float64(val.(uint64))
	case string:
		f = Atof64(val.(string), 0)
	case []byte:
		f = Atof64(string(val.([]byte)), 0)
	default:
	}

	return f
}

func AnyToBool(val any) bool {
	if val == nil {
		return false
	}

	var b bool
	var s string
	switch val.(type) {
	case bool:
		b = val.(bool)
	case int64, uint64, int, uint, int8, uint8, int16, uint16, int32, uint32:
		b = AnyToInt64(val) != 0
	case float32, float64:
		b = AnyToFloat64(val) != 0
	case string:
		s = val.(string)
	case []byte:
		s = string(val.([]byte))
	default:

	}

	if s != "" {
		switch strings.ToLower(s) {
		case "true", "1", "yes", "y", "on", "-1":
			b = true
		}
	}

	return b
}

func AnyToString(val any) string {
	if val == nil {
		return ""
	}
	var s string
	switch val.(type) {
	case bool:
		if val.(bool) {
			s = "true"
		} else {
			s = "false"
		}
	case int64, int, uint16, int8, int16, int32:
		s = I64toa(AnyToInt64(val))
	case uint64, uint, uint8, uint32:
		s = U64toa(AnyToUint64(val))
	case float32, float64:
		s = Ftoa(AnyToFloat64(val))
	case string:
		s = val.(string)
	case []byte:
		s = string(val.([]byte))
	default:

	}

	return s
}
