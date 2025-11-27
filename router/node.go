package router

import (
	"context"

	"distributedstore/lsm"
	"distributedstore/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type NodeServer struct {
	proto.UnimplementedNodeServiceServer
	db *lsm.DB
}

func NewNodeServer(db *lsm.DB) *NodeServer {
	return &NodeServer{db: db}
}

func (s *NodeServer) Put(ctx context.Context, req *proto.PutRequest) (*proto.PutResponse, error) {
	s.db.Put(string(req.Kv.Key), string(req.Kv.Value))
	return &proto.PutResponse{Success: true}, nil
}

func (s *NodeServer) Get(ctx context.Context, req *proto.GetRequest) (*proto.GetResponse, error) {
	value, found := s.db.Get(string(req.Key))
	if !found {
		return nil, status.Error(codes.NotFound, "key not found")
	}
	return &proto.GetResponse{
		Kv: &proto.KeyValue{Key: req.Key, Value: []byte(value)},
	}, nil
}

func (s *NodeServer) Delete(ctx context.Context, req *proto.DeleteRequest) (*proto.DeleteResponse, error) {
	s.db.Delete(string(req.Key))
	return &proto.DeleteResponse{Success: true}, nil
}
