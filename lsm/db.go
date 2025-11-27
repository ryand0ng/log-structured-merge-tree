package lsm

import (
	"fmt"
	"path/filepath"
	"sync"
	"os"
	"bufio"
	"strings"
	"io"
)

const (
	stride = 2
	bloomM = 1024
	bloomK = 7
	dbFlushThreshold = 100 
	minCompact = 4  
)

type DB struct {
	dir string
	mu sync.RWMutex
	memtable *Memtable
	flushCh chan *Memtable
	compactCh chan struct{}
	sstables []*SSTable
	wal *WAL
	seq int
	flushThreshold int
	nextFileId int
	flushWg sync.WaitGroup
	compactWg sync.WaitGroup
	nextWalId int
	pendingFlushes int
	flushMu sync.Mutex
	flushCond *sync.Cond
}

func (db *DB) nextSeq() int {
	db.seq++
	return db.seq
}

func (db *DB) allocFileId() int {
    db.mu.Lock()
    id := db.nextFileId
    db.nextFileId++
    db.mu.Unlock()
    return id
}

func Open(dir string) *DB {
	db := &DB{
		dir: dir,
		memtable: NewMemtable(),
		sstables: []*SSTable{},
		flushThreshold: dbFlushThreshold,
		flushCh: make(chan *Memtable, 8),
		compactCh: make(chan struct{}, 1),
	}
	db.flushCond = sync.NewCond(&db.flushMu)

	walsPath := filepath.Join(dir, "wals")
	sstsPath := filepath.Join(dir, "ssts")
	os.MkdirAll(walsPath, 0o755)
	os.MkdirAll(sstsPath, 0o755)
	walMetas := discoverWALs(walsPath)

	for _, walMeta := range walMetas {
		db.seq = ReplayWAL(
			walMeta.path,
			func(s int, k string, v string) {
				db.memtable.Put(s, k, v)
			},
			func(s int, k string) {
				db.memtable.Delete(s, k)
			},
		)
	}

	lastWalId := 0
	if len(walMetas) > 0 {
		lastWalId = walMetas[len(walMetas)-1].id
	}
	nextWalId := lastWalId + 1
	db.nextWalId = nextWalId
	tables := discoverSSTables(sstsPath)
	last := 0
	for _, table := range tables {
		index, filter, seq := buildIndex(table.path)
		sstable := &SSTable{path: table.path, index: index, filter: filter}
		db.sstables = append(db.sstables, sstable)
		db.seq = max(db.seq, seq)
		last = table.id
	}
	db.nextFileId = last + 1

	walPath := filepath.Join(walsPath, fmt.Sprintf("wal-%06d.log", nextWalId))
	db.wal = OpenWAL(walPath)
	db.memtable.walPath = walPath
	
	db.flushWg.Add(1)
	go db.flusher()

	db.compactWg.Add(1)
    go db.compactor()

	return db
}

func (db *DB) Close() {
	var toFlush *Memtable

	db.mu.Lock()
	if db.memtable != nil && db.memtable.Size() > 0 {
		toFlush = db.memtable
		db.memtable = NewMemtable()
	}
	db.mu.Unlock()

	if toFlush != nil {
		db.flushCh <- toFlush
	}

	close(db.flushCh)
	db.flushWg.Wait()

	close(db.compactCh)
	db.compactWg.Wait()

	db.mu.Lock()
	defer db.mu.Unlock()
	db.wal.Sync()
}

func (db *DB) Sync() {
	db.flushMu.Lock()
	for db.pendingFlushes > 0 {
		db.flushCond.Wait()
	}
	db.flushMu.Unlock()
}

func (db *DB) Get(key string) (string, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	value, exists := db.memtable.Get(key)
	if exists {
		if len(value) == 0 {
			return "", false
		}
		return value, true
	}
	for i := len(db.sstables) - 1; i >= 0; i-- {
		sstable := db.sstables[i]

		if !sstable.filter.mightContain(key) {
			continue
		}

		last := IndexEntry{}
		found := false
		l := 0
		r := len(sstable.index) - 1
		for l <= r {
			c := (l + r) / 2
			entry := sstable.index[c]
			if entry.key <= key {
				last = entry
				found = true
				l = c + 1
			} else {
				r = c - 1
			}
		}
		if !found {
			continue
		}
		file, _ := os.Open(sstable.path)
		file.Seek(last.offset, io.SeekStart)
		sc := bufio.NewScanner(file)
		for sc.Scan() {
			line := sc.Text()
			parts := strings.SplitN(line, " ", 4)
			k := parts[2]
			if k > key {
				break
			}
			switch parts[0] {
			case "PUT":
				if key == k {
					return parts[3], true
				}
			case "DEL":
				if key == k {
					return "", false
				}
			}
		}
	}
	return "", false
}

func (db *DB) Put(key string, value string) {
	db.mu.Lock()
	seq := db.nextSeq()
	var oldMemtable *Memtable

	db.wal.WritePut(seq, key, value)
	db.wal.Sync()
	db.memtable.Put(seq, key, value)

	if db.memtable.Size() >= db.flushThreshold {
		oldMemtable = db.memtable
		oldMemtable.walPath = db.wal.path

		db.wal.Close()
		db.nextWalId++
		newWalPath := filepath.Join(db.dir, "wals", fmt.Sprintf("wal-%06d.log", db.nextWalId))
		db.wal = OpenWAL(newWalPath)

		db.memtable = NewMemtable()
		db.memtable.walPath = newWalPath
	}

	db.mu.Unlock()
	if oldMemtable != nil {
		db.flushMu.Lock()
		db.pendingFlushes++
		db.flushMu.Unlock()
		db.flushCh <- oldMemtable
	}
}

func (db *DB) Delete(key string) {
	db.mu.Lock()
	seq := db.nextSeq()
	var oldMemtable *Memtable

	db.wal.WriteDel(seq, key)
	db.wal.Sync()
	db.memtable.Delete(seq, key)

	if db.memtable.Size() >= db.flushThreshold {
		oldMemtable = db.memtable
		oldMemtable.walPath = db.wal.path

		db.wal.Close()
		db.nextWalId++
		newWalPath := filepath.Join(db.dir, "wals", fmt.Sprintf("wal-%06d.log", db.nextWalId))
		db.wal = OpenWAL(newWalPath)

		db.memtable = NewMemtable()
		db.memtable.walPath = newWalPath
	}

	db.mu.Unlock()
	if oldMemtable != nil {
		db.flushMu.Lock()
		db.pendingFlushes++
		db.flushMu.Unlock()
		db.flushCh <- oldMemtable
	}
}
