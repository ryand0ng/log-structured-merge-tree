package lsm

import (
	"crypto/rand"
	"math/big"
	"math"
)

type Kind int

const (
	KindPut Kind = iota
	KindDelete
)

type Node struct {
	key string
	value string
	seq int
	kind Kind
	forward []*Node
}

type SkipList struct {
	header *Node
	p float64
	size int
	level int
	maxLevel int
}

func NewSkipList(maxLevel int, p float64) *SkipList {
	header := &Node{forward: make([]*Node, maxLevel+1)}
	return &SkipList{
		header: header,
		level: 0,
		maxLevel: maxLevel,
		p: p,
	}
}

func (skipList *SkipList) Size() int {
	return skipList.size
}

func (skipList *SkipList) randomFloat() float64 {
	max := big.NewInt(1 << 53)
	n, _ := rand.Int(rand.Reader, max)
	return float64(n.Int64()) / float64(max.Int64())
}

func (skipList *SkipList) randomLevel() int {
	level := 0
	for level < skipList.maxLevel && skipList.randomFloat() < skipList.p {
		level++
	}
	return level
}

func (skipList *SkipList) less(a, b *Node) bool {
	if a.key != b.key {
		return a.key < b.key
	}
	if a.seq != b.seq {
		return a.seq > b.seq
	}
	return a.kind < b.kind
}

func (skipList *SkipList) Get(key string) (string, bool) {
	probe := &Node{key: key, seq: math.MaxInt, kind: KindPut}
	x := skipList.header
	for i := skipList.level; i >= 0; i-- {
		for x.forward[i] != nil && skipList.less(x.forward[i], probe) {
			x = x.forward[i]
		}
	}
	x = x.forward[0]
	if x == nil || x.key != key {
		return "", false
	}
	if x.kind == KindDelete {
		return "", true
	}
	return x.value, true
}

func (skipList *SkipList) Put(seq int, key string, value string) {
	skipList.insertInternal(key, seq, KindPut, value)
}

func (skipList *SkipList) Delete(seq int, key string) {
	skipList.insertInternal(key, seq, KindDelete, "")
}

func (skipList *SkipList) insertInternal(key string, seq int, kind Kind, value string) {
	update := make([]*Node, skipList.maxLevel + 1)
	probe := &Node{key: key, seq: seq, kind: kind}
	x := skipList.header
	for i := skipList.level; i >= 0; i-- {
		for x.forward[i] != nil && skipList.less(x.forward[i], probe) {
			x = x.forward[i]
		}
		update[i] = x
	}

	level := skipList.randomLevel()
	if level > skipList.level {
		for i := skipList.level + 1; i <= level; i++ {
			update[i] = skipList.header
		}
		skipList.level = level
	}
	newNode := &Node{
		key: key,
		seq: seq,
		kind: kind,
		value: value,
		forward: make([]*Node, level+1),
	}

	for i := 0; i <= level; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}
	skipList.size++
}
