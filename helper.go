package djoemo

func valueFromPtr[T any](ptr *T) T {
	if ptr == nil {
		var v T
		return v
	}

	return *ptr
}
