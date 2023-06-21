package slices

func Contains[T comparable](elems []T, v T) bool {
	for _, elem := range elems {
		if elem == v {
			return true
		}
	}

	return false
}
