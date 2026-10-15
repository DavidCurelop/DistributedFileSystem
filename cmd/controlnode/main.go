package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"sort"
	"sync"
	"time"

	pb "distributedFileSystem/pkg/proto/client"
	cnpb "distributedFileSystem/pkg/proto/controlnode"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	heartbeatIntervalSeconds = 5
	heartbeatTimeout         = 3 * heartbeatIntervalSeconds * time.Second
	monitorInterval          = heartbeatIntervalSeconds * time.Second
	replicasPerChunk         = 2
)

// nodeInfo is the ControlNode's view of a registered DataNode.
type nodeInfo struct {
	id                 string
	address            string
	totalDiskBytes     int64
	availableDiskBytes int64
	storedChunkCount   int32
	lastSeen           time.Time
}

// nodeRegistry holds the active DataNodes, guarded by an RWMutex.
type nodeRegistry struct {
	mu      sync.RWMutex
	nodes   map[string]*nodeInfo // keyed by node ID
	nextID  uint64
	timeout time.Duration
}

func newNodeRegistry(timeout time.Duration) *nodeRegistry {
	return &nodeRegistry{
		nodes:   make(map[string]*nodeInfo),
		timeout: timeout,
	}
}

// register adds a node, or refreshes it if the address is already registered.
func (r *nodeRegistry) register(address string, totalDisk, availableDisk int64) string {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	for _, n := range r.nodes {
		if n.address == address {
			n.totalDiskBytes = totalDisk
			n.availableDiskBytes = availableDisk
			n.lastSeen = now
			return n.id
		}
	}

	r.nextID++
	id := fmt.Sprintf("node-%d", r.nextID)
	r.nodes[id] = &nodeInfo{
		id:                 id,
		address:            address,
		totalDiskBytes:     totalDisk,
		availableDiskBytes: availableDisk,
		lastSeen:           now,
	}
	return id
}

// touch records a heartbeat. It returns false if the node is unknown.
func (r *nodeRegistry) touch(id string, availableDisk int64, chunkCount int32) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	n, ok := r.nodes[id]
	if !ok {
		return false
	}
	n.availableDiskBytes = availableDisk
	n.storedChunkCount = chunkCount
	n.lastSeen = time.Now()
	return true
}

// evictExpired removes nodes whose last heartbeat is older than the timeout
// and returns the evicted nodes.
func (r *nodeRegistry) evictExpired(now time.Time) []nodeInfo {
	r.mu.Lock()
	defer r.mu.Unlock()

	var evicted []nodeInfo
	for id, n := range r.nodes {
		if now.Sub(n.lastSeen) > r.timeout {
			evicted = append(evicted, *n)
			delete(r.nodes, id)
		}
	}
	return evicted
}

// activeAddresses returns the addresses of live nodes in a stable order.
func (r *nodeRegistry) activeAddresses() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	addrs := make([]string, 0, len(r.nodes))
	for _, n := range r.nodes {
		addrs = append(addrs, n.address)
	}
	sort.Strings(addrs)
	return addrs
}

// runMonitor periodically evicts nodes that missed their heartbeat deadline
// until ctx is cancelled.
func (r *nodeRegistry) runMonitor(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			for _, n := range r.evictExpired(now) {
				log.Printf("DataNode %s (%s) unregistered: no heartbeat for %v", n.id, n.address, r.timeout)
			}
		}
	}
}

// controlNodeServer implements the file-saving service and the node management service.
type controlNodeServer struct {
	pb.UnimplementedFileSavingServer
	cnpb.UnimplementedNodeManagementServiceServer
	registry *nodeRegistry
}

func (s *controlNodeServer) RequestNodeIPs(
	ctx context.Context,
	req *pb.RequestNodeIPsRequest,
) (*pb.RequestNodeIPsResponse, error) {
	log.Printf("Received allocation request for file %q (%d chunks)", req.FileName, req.ChunkNumber)

	activeIPs := s.registry.activeAddresses()
	nodeNumber := len(activeIPs)
	if nodeNumber == 0 {
		return nil, status.Error(codes.Unavailable, "no active DataNodes available")
	}

	replicas := replicasPerChunk
	if nodeNumber < replicas {
		replicas = nodeNumber
	}

	chunkNumber := int(req.ChunkNumber)
	IPAddress := make([]*pb.ReplicaSet, 0, chunkNumber)
	for i := 0; i < chunkNumber; i++ {
		chunkIPs := make([]string, 0, replicas)
		for r := 0; r < replicas; r++ {
			chunkIPs = append(chunkIPs, activeIPs[(i+r)%nodeNumber])
		}
		IPAddress = append(IPAddress, &pb.ReplicaSet{NodeIps: chunkIPs})
	}

	return &pb.RequestNodeIPsResponse{IpAddress: IPAddress}, nil
}

func (s *controlNodeServer) RegisterNode(
	ctx context.Context,
	req *cnpb.RegisterNodeRequest,
) (*cnpb.RegisterNodeResponse, error) {
	if req.NodeAddress == "" {
		return nil, status.Error(codes.InvalidArgument, "node_address is required")
	}

	id := s.registry.register(req.NodeAddress, req.TotalDiskBytes, req.AvailableDiskBytes)
	log.Printf("DataNode %s registered at %s", id, req.NodeAddress)

	return &cnpb.RegisterNodeResponse{
		NodeId:                   id,
		HeartbeatIntervalSeconds: heartbeatIntervalSeconds,
	}, nil
}

func (s *controlNodeServer) Heartbeat(
	ctx context.Context,
	req *cnpb.HeartbeatRequest,
) (*cnpb.HeartbeatResponse, error) {
	if req.NodeId == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}

	if !s.registry.touch(req.NodeId, req.AvailableDiskBytes, req.StoredChunkCount) {
		// Unknown (expired or ControlNode restarted): ask the node to register again.
		return &cnpb.HeartbeatResponse{Acknowledged: false, ReregisterRequired: true}, nil
	}
	return &cnpb.HeartbeatResponse{Acknowledged: true}, nil
}

func main() {
	port := ":50051"
	controlNodeListener, err := net.Listen("tcp", port)

	if err != nil {
		log.Fatalf("listening on tcp address %q: %v", port, err)
	}

	registry := newNodeRegistry(heartbeatTimeout)
	monitorCtx, stopMonitor := context.WithCancel(context.Background())
	defer stopMonitor()
	go registry.runMonitor(monitorCtx, monitorInterval)

	controlServer := grpc.NewServer()
	controlNodeServer := &controlNodeServer{registry: registry}
	pb.RegisterFileSavingServer(controlServer, controlNodeServer)
	cnpb.RegisterNodeManagementServiceServer(controlServer, controlNodeServer)

	if err := controlServer.Serve(controlNodeListener); err != nil {
		log.Fatalf("Failed to serve traffic: %v", err)
	}
}
