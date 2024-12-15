package util

func Curry[T, U, V any](
	f func(T, U) V,
) func(T) func(U) V {
	return func(t T) func(U) V {
		return func(u U) V {
			return f(t, u)
		}
	}
}
