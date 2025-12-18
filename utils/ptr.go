package utils

func PtrBool(b bool) *bool {
	return &b
}

func PtrInt64(i int) *int64 {
	v := int64(i)
	return &v
}

func PtrInt(i int) *int {
	return &i
}
