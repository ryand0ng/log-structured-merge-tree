# Distributed LSM Store

A distributed key-value store built on LSM trees in Go.

## Features

- **Skip List Memtable**: In-memory sorted structure for fast writes
- **Write-Ahead Log**: Durability via sequential disk writes
- **SSTables**: Immutable sorted files with sparse indexing
- **Bloom Filters**: Probabilistic structure to skip unnecessary disk reads
- **Background Compaction**: Merges SSTables to reclaim space and reduce read amplification
- **Distributed Routing**: Hash-based key partitioning across gRPC nodes

## API

```go
cluster := router.NewCluster(3).
    WithBasePort(50051).
    WithHTTPPort(8080).
    WithDataDir("./data")
cluster.Open()
defer cluster.Close()

cluster.Put("key", "value")
val, found, _ := cluster.Get("key")
cluster.Delete("key")
```

## TODOs

- [ ] Consistent hashing
- [ ] Replication
