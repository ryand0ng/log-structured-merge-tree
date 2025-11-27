package router

import (
	"context"
	"hash/fnv"
)

type Router struct {
	clients []*NodeClient
}

func NewRouter(addrs []string) *Router {
	var clients []*NodeClient
	for _, addr := range addrs {
		clients = append(clients, NewNodeClient(addr))
	}
	return &Router{clients: clients}
}

func (r *Router) Close() {
	for _, c := range r.clients {
		c.Close()
	}
}

func (r *Router) Put(ctx context.Context, key, value string) {
	client := r.pickNode(key)
	client.Put(ctx, key, value)
}

func (r *Router) Get(ctx context.Context, key string) (string, bool) {
	client := r.pickNode(key)
	return client.Get(ctx, key)
}

func (r *Router) Delete(ctx context.Context, key string) {
	client := r.pickNode(key)
	client.Delete(ctx, key)
}

func (r *Router) pickNode(key string) *NodeClient {
	h := fnv.New32a()
	h.Write([]byte(key))
	idx := int(h.Sum32()) % len(r.clients)
	return r.clients[idx]
}
