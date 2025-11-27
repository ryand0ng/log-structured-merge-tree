package router

import (
	"context"

	"distributedstore/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type NodeClient struct {
	conn *grpc.ClientConn
	client proto.NodeServiceClient
}

func NewNodeClient(addr string) *NodeClient {
	conn, _ := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	return &NodeClient{
		conn: conn,
		client: proto.NewNodeServiceClient(conn),
	}
}

func (c *NodeClient) Close() {
	c.conn.Close()
}

func (c *NodeClient) Put(ctx context.Context, key, value string) {
	c.client.Put(ctx, &proto.PutRequest{
		Kv: &proto.KeyValue{Key: []byte(key), Value: []byte(value)},
	})
}

func (c *NodeClient) Get(ctx context.Context, key string) (string, bool) {
	resp, err := c.client.Get(ctx, &proto.GetRequest{Key: []byte(key)})
	if err != nil || resp.Kv == nil {
		return "", false
	}
	return string(resp.Kv.Value), true
}

func (c *NodeClient) Delete(ctx context.Context, key string) {
	c.client.Delete(ctx, &proto.DeleteRequest{Key: []byte(key)})
}
