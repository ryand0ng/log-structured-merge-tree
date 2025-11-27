package router

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"distributedstore/lsm"
	"distributedstore/proto"

	"google.golang.org/grpc"
)

type ClusterConfig struct {
	NumNodes int
	BasePort int
	HTTPPort int
	DataDir string
}

func DefaultConfig() ClusterConfig {
	return ClusterConfig{
		NumNodes: 3,
		BasePort: 50051,
		HTTPPort: 8080,
		DataDir: "./data",
	}
}

type Cluster struct {
	config ClusterConfig
	nodes []*nodeInstance
	router *Router
	httpServer *http.Server
}

type nodeInstance struct {
	db *lsm.DB
	server *grpc.Server
	listener net.Listener
	port int
	dataDir string
}

func NewCluster(numNodes int) *Cluster {
	config := DefaultConfig()
	config.NumNodes = numNodes
	return &Cluster{config: config}
}

func NewClusterWithConfig(config ClusterConfig) *Cluster {
	return &Cluster{config: config}
}

func (c *Cluster) WithHTTPPort(port int) *Cluster {
	c.config.HTTPPort = port
	return c
}

func (c *Cluster) WithBasePort(port int) *Cluster {
	c.config.BasePort = port
	return c
}

func (c *Cluster) WithDataDir(dir string) *Cluster {
	c.config.DataDir = dir
	return c
}

func (c *Cluster) Open() error {
	var nodeAddrs []string
	for i := 0; i < c.config.NumNodes; i++ {
		port := c.config.BasePort + i
		dataDir := fmt.Sprintf("%s/node%d", c.config.DataDir, i+1)

		node := c.startNode(port, dataDir)
		c.nodes = append(c.nodes, node)
		nodeAddrs = append(nodeAddrs, fmt.Sprintf("127.0.0.1:%d", port))
	}

	c.router = NewRouter(nodeAddrs)
	c.startHTTPServer()

	fmt.Printf("Cluster started: %d nodes on ports %d-%d, HTTP on :%d\n",
		c.config.NumNodes, c.config.BasePort, c.config.BasePort+c.config.NumNodes-1, c.config.HTTPPort)
	return nil
}

func (c *Cluster) startNode(port int, dataDir string) *nodeInstance {
	db := lsm.Open(dataDir)

	listener, _ := net.Listen("tcp", fmt.Sprintf(":%d", port))

	server := grpc.NewServer()
	proto.RegisterNodeServiceServer(server, NewNodeServer(db))

	go server.Serve(listener)

	return &nodeInstance{
		db: db,
		server: server,
		listener: listener,
		port: port,
		dataDir: dataDir,
	}
}

func (c *Cluster) startHTTPServer() {
	mux := http.NewServeMux()

	mux.HandleFunc("/put", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		value := r.URL.Query().Get("value")
		c.router.Put(r.Context(), key, value)
		fmt.Fprintln(w, "OK")
	})

	mux.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		value, found := c.router.Get(r.Context(), key)
		if !found {
			http.NotFound(w, r)
			return
		}
		fmt.Fprintln(w, value)
	})

	mux.HandleFunc("/delete", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		c.router.Delete(r.Context(), key)
		fmt.Fprintln(w, "OK")
	})

	c.httpServer = &http.Server{
		Addr: fmt.Sprintf(":%d", c.config.HTTPPort),
		Handler: mux,
	}

	go c.httpServer.ListenAndServe()

	time.Sleep(50 * time.Millisecond)
}

func (c *Cluster) Close() {
	if c.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		c.httpServer.Shutdown(ctx)
	}

	if c.router != nil {
		c.router.Close()
	}

	for _, node := range c.nodes {
		node.server.GracefulStop()
		node.db.Close()
	}

	fmt.Println("Cluster stopped")
}

func (c *Cluster) Put(key, value string) error {
	c.router.Put(context.Background(), key, value)
	return nil
}

func (c *Cluster) Get(key string) (string, bool, error) {
	val, found := c.router.Get(context.Background(), key)
	return val, found, nil
}

func (c *Cluster) Delete(key string) error {
	c.router.Delete(context.Background(), key)
	return nil
}

func (c *Cluster) NumNodes() int {
	return len(c.nodes)
}

func (c *Cluster) HTTPAddr() string {
	return fmt.Sprintf("http://127.0.0.1:%d", c.config.HTTPPort)
}

func (c *Cluster) Sync() {
	for _, node := range c.nodes {
		node.db.Sync()
	}
}
