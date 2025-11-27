package lsm

type Iterator interface {
	Key() string
	Seq() int
	Kind() Kind
	Value() string
	Valid() bool
	Next()
	Close()
}

type HeapItem struct {
	it Iterator
	key string
	seq int
	kind Kind
	value string
}

type IterHeap []*HeapItem

func (h IterHeap) Len() int { return len(h) }

func (h IterHeap) Less(i, j int) bool {
	if h[i].key != h[j].key {
		return h[i].key < h[j].key
	}
	return h[i].seq > h[j].seq
}

func (h IterHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *IterHeap) Push(x any) {
	*h = append(*h, x.(*HeapItem))
}

func (h *IterHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}
