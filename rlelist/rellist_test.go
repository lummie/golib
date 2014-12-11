package rlelist

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
	list := New()
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
	list := New()

	// add the items to the list
	for i := uint(0); i < 100; i++ {
		list.Append(i)
	}

	list.Iterate(func(index uint, value interface{}) {
		assert.Equal(t, index, value.(uint), "Expect the index to equal the value")
	})
}

func TestIteratorItemsInBlocks(t *testing.T) {
	list := New()

	// add the items to the list
	for i := uint(0); i < 1000; i++ {
		list.Append(uint(i / 10))
	}

	list.Iterate(func(index uint, value interface{}) {
		assert.Equal(t, uint(index/10), value.(uint), "Expect the index to equal the value")
	})
}

func TestIteratorReadWriteEmpty(t *testing.T) {
	list := New()
	buf := new(bytes.Buffer)
	list.Write(buf)
	err := list.Read(buf)
	assert.Nil(t, err, "Unexpected Error")
}

func TestIteratorReadWriteWithItemsInOneBlock(t *testing.T) {
	list := New()
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
	list := New()
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

	listItem := list.list.Front()
	assert.Equal(t, 0, listItem.Value.(*block).RowIndex, "Unexpected RowIndex")
	assert.Equal(t, 2, listItem.Value.(*block).Length, "Unexpected Length")
	assert.Equal(t, "Value 1", listItem.Value.(*block).Value, "Unexpected value")
	listItem = listItem.Next()
	assert.Equal(t, 2, listItem.Value.(*block).RowIndex, "Unexpected RowIndex")
	assert.Equal(t, 2, listItem.Value.(*block).Length, "Unexpected Length")
	assert.Equal(t, "Value 2", listItem.Value.(*block).Value, "Unexpected value")
	listItem = listItem.Next()
	assert.Equal(t, 4, listItem.Value.(*block).RowIndex, "Unexpected RowIndex")
	assert.Equal(t, 2, listItem.Value.(*block).Length, "Unexpected Length")
	assert.Equal(t, "Value 3", listItem.Value.(*block).Value, "Unexpected value")
	listItem = listItem.Next()
	assert.Equal(t, 6, listItem.Value.(*block).RowIndex, "Unexpected RowIndex")
	assert.Equal(t, 1, listItem.Value.(*block).Length, "Unexpected Length")
	assert.Equal(t, "Value 4", listItem.Value.(*block).Value, "Unexpected value")
	listItem = listItem.Next()
	assert.Equal(t, 7, listItem.Value.(*block).RowIndex, "Unexpected RowIndex")
	assert.Equal(t, 1, listItem.Value.(*block).Length, "Unexpected Length")
	assert.Equal(t, "Value 5", listItem.Value.(*block).Value, "Unexpected value")
	listItem = listItem.Next()
	assert.Nil(t, listItem, "Expected there to be no more items")

}

const fileReadWriteTestCount int = 1000000

func TestIteratorWriteToFile(t *testing.T) {
	// create the list and populate
	list := New()

	// add the items to the list
	for i := 0; i < fileReadWriteTestCount; i++ {
		list.Append("Item " + strconv.Itoa(i/10))
	}

	filename := "/tmp/RleList.dat"
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
	list := New()

	filename := "/tmp/RleList.dat"
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
}

func TestIteratorReadFromEmptyFile(t *testing.T) {
	filename := "/tmp/RleListInvalid.dat"
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

	list := New()
	err = list.Read(w)

	assert.NotNil(t, err, "Expected an error reading empty file")
	assert.Equal(t, 0, list.blockCount, "Expect no blocks")
	assert.Equal(t, 0, list.rowCount, "Expect no rows")
}

func BenchmarkAppendMod1(b *testing.B) {
	list := New()

	for i := 0; i < b.N; i++ {
		list.Append(int(i))
	}
}

func BenchmarkAppendMod10(b *testing.B) {
	list := New()

	for i := 0; i < b.N; i++ {
		list.Append(int(i / 10))
	}
}

func BenchmarkAppendMod100(b *testing.B) {
	list := New()

	for i := 0; i < b.N; i++ {
		list.Append(int(i / 100))
	}
}

func BenchmarkAppendMod1000(b *testing.B) {
	list := New()

	for i := 0; i < b.N; i++ {
		list.Append(int(i / 1000))
	}
}

func BenchmarkAppendRandomRange2(b *testing.B) {
	rand.Seed(42)
	randRange := 2

	list := New()

	for i := 0; i < b.N; i++ {
		list.Append(rand.Intn(randRange))
	}
}

func BenchmarkAppendRandomRange4(b *testing.B) {
	rand.Seed(42)
	randRange := 4

	list := New()

	for i := 0; i < b.N; i++ {
		list.Append(rand.Intn(randRange))
	}
}

func BenchmarkAppendRandomRange8(b *testing.B) {
	rand.Seed(42)
	randRange := 8

	list := New()

	for i := 0; i < b.N; i++ {
		list.Append(rand.Intn(randRange))
	}
}

func BenchmarkAppendRandomRange16(b *testing.B) {
	rand.Seed(42)
	randRange := 16

	list := New()

	for i := 0; i < b.N; i++ {
		list.Append(rand.Intn(randRange))
	}
}

func BenchmarkWriteReadSpeedToBuffer(b *testing.B) {
	list := New()
	for i := 0; i < b.N; i++ {
		list.Append(int(i / 1000))
	}

	b.ResetTimer()

	buf := new(bytes.Buffer)
	list.Write(buf)

	list.Read(buf)

}
