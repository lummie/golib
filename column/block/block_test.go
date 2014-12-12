package block

import (
	"bufio"
	"bytes"
	"github.com/lummie/golib/assert"
	"math/rand"
	"os"
	"strconv"
	"testing"
)

func TestThatBlockAndRowCountAreCorrectForAppend(t *testing.T) {
	list := New(100)
	assert.Equal(t, list.blockCount, 0, "Expect blockCount to be zero on empty list")
	assert.Equal(t, list.rowCount, 0, "Expect rowCount to be zero on empty list")

	var index uint
	index = list.Append("Value 1")
	assert.Equal(t, index, 0)
	assert.Equal(t, list.blockCount, 1, "Expect blockCount to be 1")
	assert.Equal(t, list.rowCount, 1, "Expect rowCount to be 1")

	index = list.Append("Value 2")
	assert.Equal(t, index, 1)
	assert.Equal(t, list.blockCount, 2, "Expect blockCount to be 2")
	assert.Equal(t, list.rowCount, 2, "Expect rowCount to be 2")

	index = list.Append("Value 2")
	assert.Equal(t, index, 2)
	assert.Equal(t, list.blockCount, 2, "Expect blockCount to be 2")
	assert.Equal(t, list.rowCount, 3, "Expect rowCount to be 3")
}

func TestIteratorUniqueItems(t *testing.T) {
	list := New(100)

	// add the items to the list
	for i := uint(0); i < 100; i++ {
		list.Append(i)
	}

	list.Iterate(func(index uint, value interface{}) {
		assert.Equal(t, index, value.(uint), "Expect the index to equal the value")
	})
}

func TestIteratorItemsInBlocks(t *testing.T) {
	list := New(1000)

	// add the items to the list
	for i := uint(0); i < 1000; i++ {
		list.Append(uint(i / 10))
	}

	list.Iterate(func(index uint, value interface{}) {
		assert.Equal(t, uint(index/10), value.(uint), "Expect the index to equal the value")
	})
}

func TestIteratorReadWriteEmpty(t *testing.T) {
	list := New(100)
	buf := new(bytes.Buffer)
	wErr := list.Write(buf)
	assert.Nil(t, wErr, "Unexpected Write Error")

	rErr := list.Read(buf)
	assert.Nil(t, rErr, "Unexpected Read Error")
}

func TestIteratorReadEmptyBuffer(t *testing.T) {
	list := New(100)
	buf := new(bytes.Buffer)

	rErr := list.Read(buf)
	assert.NotNil(t, rErr, "Expected Read Error")
}

func TestIteratorReadWriteWithItemsInOneBlock(t *testing.T) {
	list := New(2)
	list.Append("Value 1")
	list.Append("Value 1")

	buf := new(bytes.Buffer)
	err := list.Write(buf)
	assert.Nil(t, err, "Unexpected Write Error")

	err = list.Read(buf)
	assert.Nil(t, err, "Unexpected Read Error")
	assert.Equal(t, list.rowCount, 2, "Expected 2 rows")
	assert.Equal(t, list.blockCount, 1, "Expected 2 blocks")

	list.Iterate(func(index uint, value interface{}) {
		assert.Equal(t, "Value 1", value, "Stored Value does not match")
	})
}

func TestIteratorReadWriteWithItemsInManyBlocks(t *testing.T) {
	list := New(10)
	list.Append("Value 1")
	list.Append("Value 1")
	list.Append("Value 2")
	list.Append("Value 2")
	list.Append("Value 3")
	list.Append("Value 3")
	list.Append("Value 4")
	list.Append("Value 5")

	buf := new(bytes.Buffer)
	err := list.Write(buf)
	assert.Nil(t, err, "Unexpected Write Error")

	err = list.Read(buf)
	assert.Nil(t, err, "Unexpected Read Error")
	assert.Equal(t, list.rowCount, 8, "Expected 8 rows")
	assert.Equal(t, list.blockCount, 5, "Expected 5 blocks")

	blk := list.data[0]
	assert.Equal(t, 0, blk.RowIndex, "Unexpected RowIndex")
	assert.Equal(t, 2, blk.Length, "Unexpected Length")
	assert.Equal(t, "Value 1", blk.Value, "Unexpected value")
	blk = list.data[1]
	assert.Equal(t, 2, blk.RowIndex, "Unexpected RowIndex")
	assert.Equal(t, 2, blk.Length, "Unexpected Length")
	assert.Equal(t, "Value 2", blk.Value, "Unexpected value")
	blk = list.data[2]
	assert.Equal(t, 4, blk.RowIndex, "Unexpected RowIndex")
	assert.Equal(t, 2, blk.Length, "Unexpected Length")
	assert.Equal(t, "Value 3", blk.Value, "Unexpected value")
	blk = list.data[3]
	assert.Equal(t, 6, blk.RowIndex, "Unexpected RowIndex")
	assert.Equal(t, 1, blk.Length, "Unexpected Length")
	assert.Equal(t, "Value 4", blk.Value, "Unexpected value")
	blk = list.data[4]
	assert.Equal(t, 7, blk.RowIndex, "Unexpected RowIndex")
	assert.Equal(t, 1, blk.Length, "Unexpected Length")
	assert.Equal(t, "Value 5", blk.Value, "Unexpected value")

}

const fileReadWriteTestCount int = 1000000

func TestIteratorWriteToFile(t *testing.T) {
	// create the list and populate
	list := New(fileReadWriteTestCount)

	// add the items to the list
	for i := 0; i < fileReadWriteTestCount; i++ {
		list.Append("Item " + strconv.Itoa(i/10))
	}

	filename := "/tmp/Block.dat"
	fo, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	// close fo on exit and check for its returned error
	defer func() {
		if err := fo.Close(); err != nil {
			panic(err)
		}
	}()
	w := bufio.NewWriter(fo)
	list.Write(w)
	w.Flush()

}

func TestIteratorReadFromFile(t *testing.T) {
	// create the list and populate
	list := New(fileReadWriteTestCount)

	filename := "/tmp/Block.dat"
	f, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	// close f on exit and check for its returned error
	defer func() {
		if err := f.Close(); err != nil {
			panic(err)
		}
	}()
	w := bufio.NewReader(f)
	list.Read(w)
	var count uint = 0
	list.Iterate(func(index uint, value interface{}) {
		count++
	})
	assert.Equal(t, fileReadWriteTestCount, count, "Expected number of records:", fileReadWriteTestCount)
	os.Remove(filename)
}

func TestIteratorReadFromEmptyFile(t *testing.T) {
	filename := "/tmp/BlockInvalid.dat"
	fo, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	fo.WriteString("Invalid Data")
	fo.Close()

	f, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	// close f on exit and check for its returned error
	defer func() {
		if err := f.Close(); err != nil {
			panic(err)
		}
	}()
	w := bufio.NewReader(f)

	list := New(10)
	err = list.Read(w)

	assert.NotNil(t, err, "Expected an error reading empty file")
	assert.Equal(t, 0, list.blockCount, "Expect no blocks")
	assert.Equal(t, 0, list.rowCount, "Expect no rows")
	os.Remove(filename)
}

func TestIteratorReadFromEmptyFileDoesNotDestroyData(t *testing.T) {
	filename := "/tmp/BlockInvalid.dat"
	fo, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	fo.WriteString("Invalid Data")
	fo.Close()

	f, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	// close f on exit and check for its returned error
	defer func() {
		if err := f.Close(); err != nil {
			panic(err)
		}
	}()
	w := bufio.NewReader(f)

	// create list and add some data
	list := New(10)
	list.Append("Value 1")
	list.Append("Value 1")
	list.Append("Value 2")
	list.Append("Value 2")
	list.Append("Value 3")
	list.Append("Value 3")
	list.Append("Value 4")
	list.Append("Value 5")

	// attempt read
	err = list.Read(w)
	assert.NotNil(t, err, "Expected an error reading empty file")

	// check existing data is still correct
	assert.Equal(t, list.rowCount, 8, "Expected 8 rows")
	assert.Equal(t, list.blockCount, 5, "Expected 5 blocks")

	blk := list.data[0]
	assert.Equal(t, 0, blk.RowIndex, "Unexpected RowIndex")
	assert.Equal(t, 2, blk.Length, "Unexpected Length")
	assert.Equal(t, "Value 1", blk.Value, "Unexpected value")
	blk = list.data[1]
	assert.Equal(t, 2, blk.RowIndex, "Unexpected RowIndex")
	assert.Equal(t, 2, blk.Length, "Unexpected Length")
	assert.Equal(t, "Value 2", blk.Value, "Unexpected value")
	blk = list.data[2]
	assert.Equal(t, 4, blk.RowIndex, "Unexpected RowIndex")
	assert.Equal(t, 2, blk.Length, "Unexpected Length")
	assert.Equal(t, "Value 3", blk.Value, "Unexpected value")
	blk = list.data[3]
	assert.Equal(t, 6, blk.RowIndex, "Unexpected RowIndex")
	assert.Equal(t, 1, blk.Length, "Unexpected Length")
	assert.Equal(t, "Value 4", blk.Value, "Unexpected value")
	blk = list.data[4]
	assert.Equal(t, 7, blk.RowIndex, "Unexpected RowIndex")
	assert.Equal(t, 1, blk.Length, "Unexpected Length")
	assert.Equal(t, "Value 5", blk.Value, "Unexpected value")
	os.Remove(filename)
}

/*
	BENCH MARKING -----------------------------------------------------------------------
*/

func BenchmarkWriteReadSpeedToBuffer(b *testing.B) {
	list := New(b.N)
	for i := 0; i < b.N; i++ {
		list.Append(int(i / 1000))
	}

	b.ResetTimer()

	buf := new(bytes.Buffer)
	list.Write(buf)

	list.Read(buf)
	b.Logf("RowCount:%v BlockCount:%v", list.rowCount, list.blockCount)
}

func BenchmarkAppendMod1(b *testing.B) {
	list := New(b.N)

	for i := 0; i < b.N; i++ {
		list.Append(int(i))
	}
	b.Logf("RowCount:%v BlockCount:%v", list.rowCount, list.blockCount)
}

func BenchmarkAppendMod10(b *testing.B) {
	list := New(b.N)

	for i := 0; i < b.N; i++ {
		list.Append(int(i / 10))
	}
	b.Logf("RowCount:%v BlockCount:%v", list.rowCount, list.blockCount)
}

func BenchmarkAppendMod100(b *testing.B) {
	list := New(b.N)

	for i := 0; i < b.N; i++ {
		list.Append(int(i / 100))
	}
	b.Logf("RowCount:%v BlockCount:%v", list.rowCount, list.blockCount)
}

func BenchmarkAppendMod1000(b *testing.B) {
	list := New(b.N)

	for i := 0; i < b.N; i++ {
		list.Append(int(i / 1000))
	}
	b.Logf("RowCount:%v BlockCount:%v", list.rowCount, list.blockCount)
}

func BenchmarkAppendRandomRange2(b *testing.B) {
	rand.Seed(42)
	randRange := 2

	list := New(b.N)

	for i := 0; i < b.N; i++ {
		list.Append(rand.Intn(randRange))
	}
	b.Logf("RowCount:%v BlockCount:%v", list.rowCount, list.blockCount)
}

func BenchmarkAppendRandomRange4(b *testing.B) {
	rand.Seed(42)
	randRange := 4

	list := New(b.N)

	for i := 0; i < b.N; i++ {
		list.Append(rand.Intn(randRange))
	}
	b.Logf("RowCount:%v BlockCount:%v", list.rowCount, list.blockCount)
}

func BenchmarkAppendRandomRange8(b *testing.B) {
	rand.Seed(42)
	randRange := 8

	list := New(b.N)

	for i := 0; i < b.N; i++ {
		list.Append(rand.Intn(randRange))
	}
	b.Logf("RowCount:%v BlockCount:%v", list.rowCount, list.blockCount)
}

func BenchmarkAppendRandomRange16(b *testing.B) {
	rand.Seed(42)
	randRange := 16

	list := New(b.N)

	for i := 0; i < b.N; i++ {
		list.Append(rand.Intn(randRange))
	}
	b.Logf("RowCount:%v BlockCount:%v", list.rowCount, list.blockCount)
}

func BenchmarkAppendMod100Int(b *testing.B) {
	list := New(b.N)

	for i := 0; i < b.N; i++ {
		list.Append(int(i / 100))
	}
}

func BenchmarkAppendMod100String(b *testing.B) {
	list := New(b.N)

	for i := 0; i < b.N; i++ {
		list.Append("Item Number " + strconv.Itoa(i/100))
	}
}
