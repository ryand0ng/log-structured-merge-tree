package lsm
	
type Memtable struct {
	skipList *SkipList
	walPath string
}

func NewMemtable() *Memtable {
	return &Memtable{skipList: NewSkipList(10, 0.25)}
}

func (memtable *Memtable) Get(key string) (string, bool) {
	return memtable.skipList.Get(key)
}

func (memtable *Memtable) Put(seq int, key string, value string) {
	memtable.skipList.Put(seq, key, value)
}

func (memtable *Memtable) Delete(seq int, key string) {
	memtable.skipList.Delete(seq, key)
}

func (memtable *Memtable) Size() int {
	return memtable.skipList.Size()
}
