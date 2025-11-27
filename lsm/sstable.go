package lsm

import (
	"path/filepath"
	"os"
	"bufio"
	"strconv"
	"strings"
	"regexp"
	"sort"
)

type SSTable struct {
	path string
	index []IndexEntry
	filter *BloomFilter
}

type IndexEntry struct {
    key string
    offset int64
}

type tableMeta struct {
	id int
	path string
}

func discoverSSTables(dir string) []tableMeta {
	entries, _ := os.ReadDir(dir)

	re := regexp.MustCompile(`^sst-(\d+)\.sst$`)

	var tables []tableMeta
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

		tables = append(tables, tableMeta{
			id:   id,
			path: filepath.Join(dir, name),
		})
	}

	sort.Slice(tables, func(i, j int) bool {
		return tables[i].id < tables[j].id
	})

	return tables
}

func buildIndex(path string) ([]IndexEntry, *BloomFilter, int) {
	file, _ := os.Open(path)
	sc := bufio.NewScanner(file)
	index := []IndexEntry{}
	filter := NewBloomFilter(bloomM, bloomK)
	var offset int64
	seq := 0
	i := 0

	for sc.Scan() {
		line := sc.Text()
		bytes := int64(len(sc.Bytes())) + 1
		parts := strings.SplitN(line, " ", 4)
		key := parts[2]
		entrySeq, _ := strconv.Atoi(parts[1])
		seq = entrySeq

		filter.add(key)

		if i % stride == 0 {
			entry := IndexEntry{key: key, offset: offset}
			index = append(index, entry)
		}
		offset += bytes
		i++
	}

	return index, filter, seq
}