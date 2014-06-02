package gocdb

import (
	"bytes"
	"encoding/binary"
	"os"
	"fmt"
)

// Implemented based off of descriptions and details from the following sites:
//		http://cr.yp.to/cdb/cdb.txt
//		http://www.unixuser.org/~euske/doc/cdbinternals/index.html

//   [ <tablePointer><tablePointer>    ....    <record><record>    ...    <hashpair><hashpair> ]

type tablePointer struct {
	tablePos  uint32
	tableSize uint32
}

type record struct {
	key   []byte
	value []byte
}

type hashpair struct {
	hash uint32
	rec_ptr uint32
}

type subtable struct {
	record []hashpair
}

type ConstantDatabase struct {
	subtables []tablePointer
	database  *os.File
}

func getHash(key []byte) uint32 {
	var hash uint32 = 5381
	fmt.Printf("%d", len(key))
	for idx := range key {
		fmt.Printf("%d\n", key[idx])
		hash = ((hash << 5) + hash) ^ uint32(key[idx])
	}
	return hash
}

func readU32OrDie(file *os.File) uint32 {
	data := make([]byte, 4)
	n, err := file.Read(data)
	if n != 4 || err != nil {
		panic("Unable to read file data. Malformed file?")
	}
	return binary.LittleEndian.Uint32(data)
}


func (cdb ConstantDatabase) readTablePointer() {
	_, err := cdb.database.Seek(0, 0)
	if err != nil {
		panic(err)
	}

	for i := 0; i < 256; i++ {
		cdb.subtables[i].tablePos = readU32OrDie(cdb.database)
		cdb.subtables[i].tableSize = readU32OrDie(cdb.database)
		fmt.Printf("Table %d found table at %d, size %d\n", i, cdb.subtables[i].tablePos, cdb.subtables[i].tableSize)
	}
}

func NewConstantDatabase(filename string) *ConstantDatabase {
	tmpdb := new(ConstantDatabase)
	dbIn, err := os.Open(filename)
	if err != nil {
		panic(err)
	}

	tmpdb.database = dbIn
	tmpdb.subtables = make([]tablePointer, 256, 256)

	tmpdb.readTablePointer()

	return tmpdb
}

func NewConstantDatabaseFromMap(filename string, input map[string]string) *ConstantDatabase {
	tablePointers := make([]tablePointer, 256, 256)
	dataRecords := make([]record, len(input))
	subTables := make([][]hashpair, 256, 256)

	baseAddr := 2048

	for key, value := range input {
		newRecord := record{[]byte(key), []byte(value)}
		hash := getHash(newRecord.key)
		dataRecords.append(newRecord)
		subTables[hash&0xFF] = append(subTables[hash&0xFF], hashpair{hash, })
		//TODO: incorporate and increment baseAddr
	}


}

func (cdb ConstantDatabase) getRecordAt(offset uint32) *record {
	ret := new(record)
	cdb.database.Seek(int64(offset), 0)
	klen, vlen := readU32OrDie(cdb.database), readU32OrDie(cdb.database)
	ret.key = make([]byte, klen)
	ret.value = make([]byte, vlen)

	n, err := cdb.database.Read(ret.key)
	if n != int(klen) || err != nil {
		panic("Unable to read record key data. Malformed file?")
	}

	n, err = cdb.database.Read(ret.value)
	if n != int(vlen) || err != nil {
		panic("Unable to read record value data. Malformed file?")
	}

	return ret
}

func (cdb ConstantDatabase) Get(keyStr string) (string, bool) {
	key := []byte(keyStr)
	hash := getHash(key)

	subtable := (hash & 0xFF)
	fmt.Printf("Fetching subtable %d\n", subtable)
	fmt.Printf("Subtable size: %d\n", cdb.subtables[subtable].tableSize)
	if (cdb.subtables[subtable].tableSize == 0) {
		return "", false
	}
	slot := (hash >> 8) % (cdb.subtables[subtable].tableSize)

	for {
		pos := cdb.subtables[subtable].tablePos + slot*8
		cdb.database.Seek(int64(pos), 0)

		inHash, inPtr := readU32OrDie(cdb.database), readU32OrDie(cdb.database)
		record := cdb.getRecordAt(inPtr)
		if inHash != hash || inPtr == 0 {
			// The desired element is not here. Exit the loop
			break
		} else if bytes.Equal(record.key, key) {
			// Key in record matches desired key
			return string(record.value), true
		} else {
			// Hash collision, check the next slot
			// Also, make sure we loop around our array if necessary
			slot++
			if slot == cdb.subtables[subtable].tableSize {
				slot = 0
			}
		}
	}

	//We did not find a match :(
	return "", false
}

func main() int {
	fmt.Printf("Hw");

	return 0;
}
