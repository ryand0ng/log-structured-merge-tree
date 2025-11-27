package lsm

import (
	"hash/fnv"
)

type BloomFilter struct {
	m uint
	k uint
	bits []uint64
}

func NewBloomFilter(m, k uint) *BloomFilter {
    return &BloomFilter{
        m: m,
        k: k,
        bits: make([]uint64, (m+63)/64),
    }
}

func bloomHashes(x uint64) (uint64, uint64) {
    x += 0x9e3779b97f4a7c15
    z := x
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
    z = (z ^ (z >> 27)) * 0x94d049bb133111eb
    h1 := z ^ (z >> 31)

    x += 0x9e3779b97f4a7c15
    z = x
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
    z = (z ^ (z >> 27)) * 0x94d049bb133111eb
    h2 := z ^ (z >> 31)

    return h1, h2
}

func stringHash64(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}

func (bf *BloomFilter) add(key string) {
    if bf == nil {
        return
    }
    h1, h2 := bloomHashes(stringHash64(key))
    for i := uint(0); i < bf.k; i++ {
        idx := (h1 + uint64(i) * h2) & (uint64(bf.m) - 1)
        bf.bits[idx / 64] |= 1 << (idx % 64)
    }
}

func (bf *BloomFilter) mightContain(key string) bool {
    h1, h2 := bloomHashes(stringHash64(key))
    for i := uint(0); i < bf.k; i++ {
        idx := (h1 + uint64(i) * h2) & (uint64(bf.m) - 1)
        if bf.bits[idx / 64] & (1 << (idx % 64)) == 0 {
            return false
        }
    }
    return true
}
