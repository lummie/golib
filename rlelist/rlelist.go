package rlelist

// Implements a run-Length encoded list
// The RleList only stores that appended values if they differ from the previous value, otherwise it maintains a count of the repeated item

import (
	"container/list"
	"encoding/gob"
	"io"
	"sync"
)

type RleList struct {
	sync.RWMutex
	list       *list.List // linked list storing the rows
	blockCount uint       // number of blocks added
	rowCount   uint       // number of rows stored
}

type block struct {
	RowIndex uint        // starting row Index
	Length   uint        // number of repeats of the stored value
	Value    interface{} // value stored
}

func New() *RleList {
	return &RleList{
		list:       list.New(),
		blockCount: 0,
		rowCount:   0,
	}
}

// appends a row to the list
func (r *RleList) Append(value interface{}) uint {
	// check if the list is empty and if so add the new block
	lastBlockListItem := r.list.Back()
	if lastBlockListItem == nil {
		newBlock := &block{
			RowIndex: 0,
			Length:   1,
			Value:    value,
		}
		r.Lock()
		defer r.Unlock()
		r.list.PushBack(newBlock) // add the new block to the list
		r.blockCount += 1         // increment the number of blocks stored
		r.rowCount += 1           // increment the number of rows
		return newBlock.RowIndex
	}

	// lastBlock is assigned so compare the stored value
	lastBlock := lastBlockListItem.Value.(*block)
	if lastBlock.Value == value {
		// the value in the lastBlock is the same as the value to store so just increment then Length
		r.Lock()
		defer r.Unlock()
		lastBlock.Length += 1
		r.rowCount += 1 // increment the number of rows
		return lastBlock.RowIndex + lastBlock.Length - 1
	}

	// the value is the lastBlock is different so we need to add a new block
	newBlock := &block{
		RowIndex: lastBlock.RowIndex + lastBlock.Length,
		Length:   1,
		Value:    value,
	}
	r.Lock()
	defer r.Unlock()
	r.list.PushBack(newBlock) // add the new block to the list
	r.blockCount += 1         // increment the number of blocks stored
	r.rowCount += 1           // increment the number of rows
	return newBlock.RowIndex
}

// iterator function type delaration
type IteratorFn func(index uint, value interface{})

// iterates each row in the RleList
func (r *RleList) Iterate(f IteratorFn) {
	r.RLock()
	defer r.RUnlock()

	// for each item in the list
	for listItem := r.list.Front(); listItem != nil; listItem = listItem.Next() {
		// get the block
		block := listItem.Value.(*block)
		for row := uint(0); row < block.Length; row++ {
			// call the iterator function for the Length of the block
			f(block.RowIndex+row, block.Value)
		}
	}
}

// writes the rleList to a writer
func (r *RleList) Write(writer io.Writer) error {
	r.Lock()
	defer r.Unlock()
	enc := gob.NewEncoder(writer)

	err := enc.Encode("RLELIST")
	if err != nil {
		return err
	}

	err = enc.Encode(r.rowCount)
	if err != nil {
		return err
	}

	err = enc.Encode(r.blockCount)
	if err != nil {
		return err
	}

	for listItem := r.list.Front(); listItem != nil; listItem = listItem.Next() {
		// get the block
		block := listItem.Value.(*block)
		err = enc.Encode(block)
		if err != nil {
			return err
		}
	}

	return nil
}

// reads the RleList from a Reader, overwriting the current contents
// if an error occurs the RleList will be initialised to empty
func (r *RleList) Read(reader io.Reader) error {
	r.Lock()
	defer r.Unlock()
	dec := gob.NewDecoder(reader)

	resetToEmpty := func() {
		r.rowCount = 0
		r.blockCount = 0
		r.list = list.New()
	}

	// check we are decoding the correct type
	var typeCheck string
	err := dec.Decode(&typeCheck)
	if err != nil {
		defer resetToEmpty()
		return err
	}

	// create the new list to store decoded blocks
	r.list = list.New()

	// decode rowCount
	err = dec.Decode(&r.rowCount)
	if err != nil {
		defer resetToEmpty()
		return err
	}

	// decode blockCount
	err = dec.Decode(&r.blockCount)
	if err != nil {
		defer resetToEmpty()
		return err
	}

	for i := uint(0); i < r.blockCount; i++ {
		newBlock := &block{}
		err = dec.Decode(&newBlock)
		if err != nil {
			defer resetToEmpty()
			return err
		}
		r.list.PushBack(newBlock)
	}

	return nil
}
