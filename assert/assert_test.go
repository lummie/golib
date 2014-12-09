package assert

import (
	"testing"
)

type temp struct {
	a int
	b int
}

func TestAssertPackageEqual(t *testing.T) {
	message := "WARNING : Failure in assert package, other tests results may not be correct"
	Equal(t, true, true, message)
	Equal(t, 1, 1, message)
	Equal(t, "hello", "hello", message)

	a := temp{1, 1}
	b := temp{1, 1}
	Equal(t, a, b, message)

	arrayA := []string{"a", "b", "c"}
	arrayB := []string{"a", "b", "c"}
	Equal(t, arrayA, arrayB, message)
}

func TestAssertPackageNotEqual(t *testing.T) {
	message := "WARNING : Failure in assert package, other tests results may not be correct"
	NotEqual(t, false, true, message)
	NotEqual(t, 0, 1, message)
	NotEqual(t, "xxx", "hello", message)

	a := temp{1, 1}
	b := temp{1, 2}
	NotEqual(t, a, b, message)

	arrayA := []string{"a", "b", "c"}
	arrayB := []string{"a", "b", "d"}
	NotEqual(t, arrayA, arrayB, message)
}

func TestAssertPackageNilNotNil(t *testing.T) {
	message := "WARNING : Failure in assert package, other tests results may not be correct"
	Nil(t, nil, message)
	NotNil(t, true, message)
}
