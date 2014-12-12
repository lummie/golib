package block

import (
	"bytes"
	"encoding/gob"
	"errors"
	"io"
	"sync"
)

type Block struct {
	sync.RWMutex
	data       []*rlBlock
	blockCount uint // number of blocks added
	rowCount   uint // number of rows stored
}

type rlBlock struct {
	RowIndex uint        // starting row Index
	Length   uint        // number of repeats of the stored value
	Value    interface{} // value stored
}

// Creates a new Block instance
// Capacity is the initial array capacity
func New(capacity int) *Block {
	return &Block{
		data:       make([]*rlBlock, 0, capacity),
		blockCount: 0,
		rowCount:   0,
	}
}

func (r *Block) Append(value interface{}) uint {
	// check if the list is empty and if so add the new rlBlock
	if len(r.data) == 0 {
		newBlock := &rlBlock{
			RowIndex: 0,
			Length:   1,
			Value:    value,
		}

		r.Lock()
		defer r.Unlock()
		r.data = append(r.data, newBlock)
		r.blockCount += 1 // increment the number of blocks stored
		r.rowCount += 1   // increment the number of rows
		return newBlock.RowIndex
	}

	r.Lock() // lock for write
	defer r.Unlock()

	// get the lastblock
	lastBlock := r.data[len(r.data)-1]
	if lastBlock.Value == value {
		// the value in the lastBlock is the same as the value to store so just increment then Length
		lastBlock.Length += 1
		r.rowCount += 1 // increment the number of rows
		return lastBlock.RowIndex + lastBlock.Length - 1
	}

	// the value is the lastBlock is different so we need to add a new rlBlock
	newBlock := &rlBlock{
		RowIndex: lastBlock.RowIndex + lastBlock.Length,
		Length:   1,
		Value:    value,
	}
	r.data = append(r.data, newBlock) // add the new rlBlock
	r.blockCount += 1                 // increment the number of blocks stored
	r.rowCount += 1                   // increment the number of rows
	return newBlock.RowIndex
}

/*
----------------------------------------------------------------------------------------------------------------------------------------
	ITERATION and LOCATION
----------------------------------------------------------------------------------------------------------------------------------------
*/

// iterator function type delaration
type IteratorFn func(index uint, value interface{})

// iterates each row in the RleList
func (r *Block) Iterate(f IteratorFn) {
	r.RLock()
	defer r.RUnlock()

	for _, b := range r.data {
		for row := uint(0); row < b.Length; row++ {
			// call the iterator function for the Length of the rlBlock
			f(b.RowIndex+row, b.Value)
		}
	}
}

/*
----------------------------------------------------------------------------------------------------------------------------------------
	PERSISTENCE
----------------------------------------------------------------------------------------------------------------------------------------
*/

// Encodes the Block in GOB format to a byte array
func (r *Block) GobEncode() ([]byte, error) {
	buf := new(bytes.Buffer)
	encoder := gob.NewEncoder(buf)

	err := encoder.Encode(&r.blockCount)
	if err != nil {
		return nil, err
	}

	err = encoder.Encode(&r.rowCount)
	if err != nil {
		return nil, err
	}

	err = encoder.Encode(&r.data)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// Decodes the byte array in GOB format to the Block
func (r *Block) GobDecode(buf []byte) error {
	tBuf := bytes.NewBuffer(buf)
	decoder := gob.NewDecoder(tBuf)

	var blockCount uint
	err := decoder.Decode(&blockCount)
	if err != nil {
		return err
	}

	var rowCount uint
	err = decoder.Decode(&rowCount)
	if err != nil {
		return err
	}

	var data []*rlBlock
	err = decoder.Decode(&data)
	if err != nil {
		return err
	}

	r.rowCount = rowCount
	r.blockCount = blockCount
	r.data = data
	return nil
}

// writes the rleList to a writer
func (r *Block) Write(writer io.Writer) error {
	r.Lock()
	defer r.Unlock()
	enc := gob.NewEncoder(writer)

	err := enc.Encode("RLEARRAY")
	if err != nil {
		return err
	}

	err = enc.Encode(r)
	if err != nil {
		return err
	}

	return nil
}

// reads the RleList from a Reader, overwriting the current contents
// if an error occurs the RleList will be initialised to empty
func (r *Block) Read(reader io.Reader) error {
	r.Lock()
	defer r.Unlock()
	dec := gob.NewDecoder(reader)

	// check we are decoding the correct type
	var typeCheck string
	err := dec.Decode(&typeCheck)
	if err != nil {
		return err
	}

	if typeCheck != "RLEARRAY" {
		return errors.New("Tried to load a stream that is not RLEARRAY")
	}

	// decode rowCount
	err = dec.Decode(&r)
	if err != nil {
		return err
	}
	return nil
}
