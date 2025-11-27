package lsm

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"path/filepath"
	"sort"
	"regexp"
)

type WAL struct {
	file *os.File
	writer *bufio.Writer
	path string
}

func OpenWAL(path string) *WAL {
	file, _ := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	return &WAL{file: file, writer: bufio.NewWriterSize(file, 64<<10), path: path}
}

func (wal *WAL) Close() {
	wal.writer.Flush()
	wal.file.Close()
}

func (wal *WAL) Sync() {
	wal.writer.Flush()
	wal.file.Sync()
}

func (wal *WAL) WriteDel(seq int, key string) {
	fmt.Fprintf(wal.writer, "DEL %d %s\n", seq, key)
}

func (wal *WAL) WritePut(seq int, key string, value string) {
	fmt.Fprintf(wal.writer, "PUT %d %s %s\n", seq, key, value)
}

func ReplayWAL(path string, onPut func(int, string, string), onDel func(int, string)) int {
	file, _ := os.Open(path)
	defer file.Close()
	maxSeq := 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		parts := strings.SplitN(line, " ", 4)
		switch parts[0] {
		case "PUT":
			seq, _ := strconv.Atoi(parts[1])
			key := parts[2]
			maxSeq = max(maxSeq, seq)
			onPut(seq, key, parts[3])
		case "DEL":
			seq, _ := strconv.Atoi(parts[1])
			key := parts[2]
			maxSeq = max(maxSeq, seq)
			onDel(seq, key)
		}
	}
	return maxSeq
}

func discoverWALs(dir string) []tableMeta {
    entries, _ := os.ReadDir(dir)
    re := regexp.MustCompile(`^wal-(\d+)\.log$`)

    var wals []tableMeta
    for _, e := range entries {
        if e.IsDir() {
            continue
        }
        name := e.Name()
        m := re.FindStringSubmatch(name)
        if m == nil {
            continue
        }
        id, _ := strconv.Atoi(m[1])
        wals = append(wals, tableMeta{
            id:   id,
            path: filepath.Join(dir, name),
        })
    }

    sort.Slice(wals, func(i, j int) bool {
        return wals[i].id < wals[j].id
    })
    return wals
}
