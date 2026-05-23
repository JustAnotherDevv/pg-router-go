package util

// IsIdentByte returns true for bytes that can appear in SQL identifiers
// (letters, digits, underscore).
func IsIdentByte(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9') || c == '_'
}

// EqFold returns true if a equals b case-insensitively.
// a and b must be ASCII identifier bytes.
func EqFold(a, b byte) bool {
	return a == b || (a|0x20) == (b|0x20)
}
