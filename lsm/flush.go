package lsm

import (
	"fmt"
	"os"
	"bufio"
	"path/filepath"
)

func (db *DB) flusher() {
	defer db.flushWg.Done()
	for memtable := range db.flushCh { 
		id := db.allocFileId()
		tmp := filepath.Join(db.dir, "ssts", fmt.Sprintf("sst-%06d.tmp", id))
		target := filepath.Join(db.dir, "ssts", fmt.Sprintf("sst-%06d.sst", id))
		filter := NewBloomFilter(bloomM, bloomK)
		sstable := &SSTable{path: target, index: []IndexEntry{}, filter: filter}
		file, _ := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		var offset int64

		writer := bufio.NewWriterSize(file, 64<<10)
		x := memtable.skipList.header.forward[0]
		i := 0
		for x != nil {
			key := x.key
			var line string
			if x.kind == KindPut {
				line = fmt.Sprintf("PUT %d %s %s\n", x.seq, x.key, x.value)
			} else {
				line = fmt.Sprintf("DEL %d %s\n", x.seq, x.key)
			}
			if i % stride == 0 {
				sstable.index = append(sstable.index, IndexEntry{key: key, offset: offset})
			}

			n, _ := writer.WriteString(line)
			filter.add(key)
			offset += int64(n)

			for x != nil && x.key == key {
				x = x.forward[0]
			}
			i += 1
		}

		writer.Flush()
		file.Sync()
		file.Close()
		os.Rename(tmp, target)
		os.Remove(memtable.walPath)

		db.mu.Lock()
		db.sstables = append(db.sstables, sstable)
		compact := len(db.sstables) >= minCompact
		db.mu.Unlock()

		db.flushMu.Lock()
		db.pendingFlushes--
		db.flushCond.Broadcast()
		db.flushMu.Unlock()

		if compact {
			db.compactCh <- struct{}{}
		}
	}
}
