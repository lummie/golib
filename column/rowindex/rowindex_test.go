package rowindex

import (
	//	"github.com/lummie/golib/assert"
	"testing"
)

func TestAppendToRowIndex(t *testing.T) {
	ri := New()
	ri.Append(0, 1)
	ri.Append(1, 10)
	ri.Append(40, 10)
}
