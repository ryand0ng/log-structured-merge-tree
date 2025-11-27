package lsm

import (
	"container/heap"
	"fmt"
	"os"
	"bufio"
	"path/filepath"
)

func (db *DB) compactor() {
    defer db.compactWg.Done()
    for range db.compactCh {
        db.compact()
    }
}

func (db *DB) compact() {
	h := &IterHeap{}
	heap.Init(h)

	db.mu.RLock()
	tables := append([]*SSTable(nil), db.sstables...)
	db.mu.RUnlock()

	for _, sstable := range tables {
		it := NewSSTableIter(sstable.path)
		defer it.Close()
		it.Next()
		if it.Valid() {
			heap.Push(h, &HeapItem{
				it: it,
				key: it.Key(),
				seq: it.Seq(),
				kind: it.Kind(),
				value: it.Value(),
			})
		} 
	}

	id := db.allocFileId()
	tmp := filepath.Join(db.dir, "ssts", fmt.Sprintf("sst-%06d.compact.tmp", id))
	target := filepath.Join(db.dir, "ssts", fmt.Sprintf("sst-%06d.sst", id))
	filter := NewBloomFilter(bloomM, bloomK)
	sstable := &SSTable{path: target, index: []IndexEntry{}, filter: filter}
	file, _ := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	writer := bufio.NewWriterSize(file, 64<<10)

	var offset int64
	i := 0

	for h.Len() > 0 {
		item := heap.Pop(h).(*HeapItem)
		currentKey := item.key
		newestKind := item.kind
		newestSeq := item.seq
		newestVal := item.value

		item.it.Next()
		if item.it.Valid() {
			heap.Push(h, &HeapItem{
				it: item.it,
				key: item.it.Key(),
				seq: item.it.Seq(),
				kind: item.it.Kind(),
				value: item.it.Value(),
			})
		}

		for h.Len() > 0 {
			top := (*h)[0]
			if top.key != currentKey {
				break
			}

			older := heap.Pop(h).(*HeapItem)

			older.it.Next()
			if older.it.Valid() {
				heap.Push(h, &HeapItem{
					it: older.it,
					key: older.it.Key(),
					seq: older.it.Seq(),
					kind: older.it.Kind(),
					value: older.it.Value(),
				})
			}
		}

		if newestKind == KindPut {
			line := fmt.Sprintf("PUT %d %s %s\n", newestSeq, currentKey, newestVal)

			filter.add(currentKey)

			if i % stride == 0 {
				sstable.index = append(sstable.index, IndexEntry{
					key: currentKey,
					offset: offset,
				})
			}
			n, _ := writer.WriteString(line)
			offset += int64(n)
			i++
		} 
	}

	writer.Flush()
	file.Sync()
	file.Close()
	os.Rename(tmp, target)

	compacted := make(map[*SSTable]struct{}, len(tables))
	for _, t := range tables {
		compacted[t] = struct{}{}
	}

	db.mu.Lock()
    current := db.sstables
	var keep []*SSTable
	for _, t := range current {
		if _, wasCompacted := compacted[t]; !wasCompacted {
			keep = append(keep, t)
		}
	}
	db.sstables = append(keep, sstable)
    db.mu.Unlock()

	for _, t := range tables {
        if t.path != target { 
            _ = os.Remove(t.path)
        }
    }
}