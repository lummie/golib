package rlelist

// Implements a run-length encoded list
// The RleList only stores that appended values if they differ from the previous value, otherwise it maintains a count of the repeated item

import (
	"container/list"
	"sync"
)

type RleList struct {
	sync.RWMutex
	list       *list.List // linked list storing the rows
	blockCount uint       // number of blocks added
	rowCount   uint       // number of rows stored
}

type block struct {
	rowIndex uint        // starting row Index
	length   uint        // number of repeats of the stored value
	value    interface{} // value stored
}

func New() *RleList {
	return &RleList{
		list:       list.New(),
		blockCount: 0,
		rowCount:   0,
	}
}

func (r *RleList) Append(value interface{}) uint {
	// check if the list is empty and if so add the new block
	lastBlockListItem := r.list.Back()
	if lastBlockListItem == nil {
		newBlock := &block{
			rowIndex: 0,
			length:   1,
			value:    value,
		}
		r.Lock()
		defer r.Unlock()
		r.list.PushBack(newBlock) // add the new block to the list
		r.blockCount += 1         // increment the number of blocks stored
		r.rowCount += 1           // increment the number of rows
		return newBlock.rowIndex
	}

	// lastBlock is assigned so compare the stored value
	lastBlock := lastBlockListItem.Value.(*block)
	if lastBlock.value == value {
		// the value in the lastBlock is the same as the value to store so just increment then length
		r.Lock()
		defer r.Unlock()
		lastBlock.length += 1
		r.rowCount += 1 // increment the number of rows
		return lastBlock.rowIndex + lastBlock.length - 1
	}

	// the value is the lastBlock is different so we need to add a new block
	newBlock := &block{
		rowIndex: lastBlock.rowIndex + lastBlock.length,
		length:   1,
		value:    value,
	}
	r.Lock()
	defer r.Unlock()
	r.list.PushBack(newBlock) // add the new block to the list
	r.blockCount += 1         // increment the number of blocks stored
	r.rowCount += 1           // increment the number of rows
	return newBlock.rowIndex
}

type IteratorFn func(index uint, value interface{})

func (r *RleList) Iterate(f IteratorFn) {
	r.RLock()
	defer r.RUnlock()

	// for each item in the list
	for listItem := r.list.Front(); listItem != nil; listItem = listItem.Next() {
		// get the block
		block := listItem.Value.(*block)
		for row := uint(0); row < block.length; row++ {
			// call the iterator function for the length of the block
			f(block.rowIndex+row, block.value)
		}
	}
}
