package conv

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
)

func IsInt(val string) bool {
	_, err := strconv.Atoi(val)
	return err == nil
}

func IsInt64(val string) bool {
	_, err := strconv.ParseInt(val, 10, 64)
	return err == nil
}

func IsUint64(val string) bool {
	_, err := strconv.ParseUint(val, 10, 64)
	return err == nil
}

func ParseInt(s string, base int, bitSize int, _default int64) int64 {
	if i, err := strconv.ParseInt(s, base, bitSize); err == nil {
		return i
	}

	return _default
}

func ParseUint(s string, base int, bitSize int, _default uint64) uint64 {
	if i, err := strconv.ParseUint(s, base, bitSize); err == nil {
		return i
	}

	return _default
}

func ParseFloat(s string, bitSize int, _default float64) float64 {
	if f, err := strconv.ParseFloat(s, bitSize); err == nil {
		return f
	}
	return _default
}

func Atoi(s string, _default int) int {
	if i, err := strconv.Atoi(s); err == nil {
		return i
	}

	return _default
}

func Atoi64(s string, _default int64) int64 {
	return ParseInt(s, 10, 64, _default)
}

func Atou64(s string, _default uint64) uint64 {
	return ParseUint(s, 10, 64, _default)
}

func Atof(s string, _default float32) float32 {
	return float32(ParseFloat(s, 32, float64(_default)))
}

func Atof64(s string, _default float64) float64 {
	return ParseFloat(s, 32, _default)
}

func Ftoa(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}

func Itoa(i int) string {
	return strconv.Itoa(i)
}

func I64toa(i int64) string {
	return strconv.FormatInt(i, 10)
}

func U64toa(i uint64) string {
	return strconv.FormatUint(i, 10)
}

func HexToInt(_hex string) int64 {
	return ParseInt(_hex, 16, 64, 0)
}

func HexToBytes(_hex string) ([]byte, error) {
	return hex.DecodeString(_hex)
}

func BytesToHex(bytes []byte) string {
	return hex.EncodeToString(bytes)
}

func IntToHex(i int64) string {
	return strconv.FormatInt(i, 16)
}

// PercentageToFloat XX.xx% => 0.XXxx
func PercentageToFloat(p string) float32 {
	return Atof(strings.TrimSuffix(p, "%"), 0) / 100
}

// PaddingInt64 left padding to an int64 via "0", return a string of the length your defined
func PaddingInt64(i int64, length int8) string {
	return fmt.Sprintf("%0"+Itoa(int(length))+"d", i)
}

// PaddingUint64 left padding to an uint64 via "0", return a string of the length your defined
func PaddingUint64(i uint64, length int8) string {
	return fmt.Sprintf("%0"+Itoa(int(length))+"d", i)
}
