package rowindex

import (
	"sync"
)

type RowIndex struct {
	sync.RWMutex
	items []*indexItem
}

type indexItem struct {
	index  uint
	length uint
}

func New() *RowIndex {
	return &RowIndex{}
}

func (r *RowIndex) Append(index uint, length uint) {
	r.Lock()
	defer r.Unlock()
	r.items = append(r.items, &indexItem{
		index:  index,
		length: length,
	})
}
