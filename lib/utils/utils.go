package utils

// ToCmdLine convert strings to [][]byte
func ToCmdLine(cmd ...string) [][]byte {
	args := make([][]byte, len(cmd))
	for i, s := range cmd {
		args[i] = []byte(s)
	}
	return args
}

// ToCmdLine2 convert commandName and []byte-type argument to CmdLine
func ToCmdLine2(commandName string, args ...[]byte) [][]byte {
	result := make([][]byte, len(args)+1)
	result[0] = []byte(commandName)
	for i, s := range args {
		result[i+1] = s
	}
	return result
}

// BytesEquals check whether the given bytes is equal
func BytesEquals(a []byte, b []byte) bool {
	if (a == nil && b != nil) || (a != nil && b == nil) {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	size := len(a)
	for i := 0; i < size; i++ {
		av := a[i]
		bv := b[i]
		if av != bv {
			return false
		}
	}
	return true
}

// If 返回值 v1 或 v2 取决于 cond 是否为 true
func If(cond bool, v1, v2 interface{}) interface{} {
	if cond {
		return v1
	}
	return v2
}

// IfLazy 返回 f1() 或 f2() 取决于 cond 是否为 true
func IfLazy(cond bool, f1, f2 func() interface{}) interface{} {
	if cond {
		return f1()
	}
	return f2()
}
