package lsm

import (
	"os"
	"bufio"
	"strconv"
	"strings"
)

type SSTableIter struct {
    file *os.File
    sc *bufio.Scanner
    key string
    seq int
    value string
    kind Kind
	valid bool
}

func NewSSTableIter(path string) *SSTableIter {
    file, _ := os.Open(path)
    return &SSTableIter{
        file: file,
        sc: bufio.NewScanner(file),
		valid: true,
    }
}

func (it *SSTableIter) Next() {
    if !it.sc.Scan() {
		it.valid = false 
		return
	}

	line := it.sc.Text()
	parts := strings.SplitN(line, " ", 4)

	op := parts[0]
	seq, _ := strconv.Atoi(parts[1])
	key := parts[2]

	it.seq = seq
	it.key = key
	if op == "PUT" {
		it.kind = KindPut
		it.value = parts[3]
	} else {
		it.kind = KindDelete
		it.value = ""
	}
}

func (it *SSTableIter) Key() string { return it.key }
func (it *SSTableIter) Seq() int { return it.seq }
func (it *SSTableIter) Kind() Kind { return it.kind }
func (it *SSTableIter) Value() string { return it.value }
func (it *SSTableIter) Valid() bool { return it.valid }
func (it *SSTableIter) Close() { it.file.Close() }
