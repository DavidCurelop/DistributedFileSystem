package main

import (
	"context"
	"fmt"
	"log"
	"net"
	pathpkg "path"
	"sort"
	"strings"
	"sync"
	"time"

	pb "distributedFileSystem/pkg/proto/client"
	cnpb "distributedFileSystem/pkg/proto/controlnode"
	dnpb "distributedFileSystem/pkg/proto/datanode"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
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

type replicationJob struct {
	filePath              string
	chunkID               string
	sourceDataNodeAddress string
	targetDataNodeAddress string
}

func main() {
	port := ":50051"
	controlNodeListener, err := net.Listen("tcp", port)

	if err != nil {
		log.Fatalf("listening on tcp address %q: %v", port, err)
	}

	registry := newNodeRegistry(heartbeatTimeout)
	files := newFileRegistry()
	monitorCtx, stopMonitor := context.WithCancel(context.Background())
	defer stopMonitor()
	go registry.runMonitor(monitorCtx, monitorInterval, files)

	controlServer := grpc.NewServer()
	controlNodeServer := &controlNodeServer{registry: registry, files: files}
	pb.RegisterFileSavingServer(controlServer, controlNodeServer)
	cnpb.RegisterNodeManagementServiceServer(controlServer, controlNodeServer)
	cnpb.RegisterFileMetadataServiceServer(controlServer, controlNodeServer)

	if err := controlServer.Serve(controlNodeListener); err != nil {
		log.Fatalf("Failed to serve traffic: %v", err)
	}
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
func (r *nodeRegistry) runMonitor(ctx context.Context, interval time.Duration, files *fileRegistry) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			evictedNodes := r.evictExpired(now)
			for _, n := range evictedNodes {
				log.Printf("DataNode %s (%s) unregistered: no heartbeat for %v", n.id, n.address, r.timeout)
			}

			if len(evictedNodes) > 0 {
				replicationJobs := files.findUnderReplicatedChunks(r.activeAddresses(), replicasPerChunk)

				for _, job := range replicationJobs {
					err := executeReplicationJob(ctx, job)
					if err != nil {
						continue
					}
					files.addReplica(job.filePath, job.chunkID, job.targetDataNodeAddress)
				}
			}
		}
	}
}

// namespaceNode is a directory or a file in the in-memory namespace tree.
type namespaceNode struct {
	isDir     bool
	createdAt int64
	children  map[string]*namespaceNode // directories only
	metadata  *cnpb.FileMetadata        // files only
}

// fileRegistry is the in-memory POSIX-like namespace (directories and files with
// their chunk layout), guarded by an RWMutex. All paths passed to its methods
// must already be cleaned and absolute (see cleanFilePath and cleanDirPath).
type fileRegistry struct {
	mu   sync.RWMutex
	root *namespaceNode
}

func newFileRegistry() *fileRegistry {
	return &fileRegistry{root: newDirNode()}
}

func newDirNode() *namespaceNode {
	return &namespaceNode{
		isDir:     true,
		createdAt: time.Now().Unix(),
		children:  make(map[string]*namespaceNode),
	}
}

// splitPath turns a cleaned absolute path into its components ("/" has none).
func splitPath(cleaned string) []string {
	if cleaned == "/" {
		return nil
	}
	return strings.Split(cleaned[1:], "/")
}

func joinPath(parts []string) string {
	return "/" + strings.Join(parts, "/")
}

// walk resolves parts from the root. Callers must hold r.mu.
func (r *fileRegistry) walk(parts []string) (*namespaceNode, error) {
	current := r.root
	for i, name := range parts {
		if !current.isDir {
			return nil, status.Errorf(codes.FailedPrecondition, "%q is not a directory", joinPath(parts[:i]))
		}
		next, ok := current.children[name]
		if !ok {
			return nil, status.Errorf(codes.NotFound, "%q does not exist", joinPath(parts[:i+1]))
		}
		current = next
	}
	return current, nil
}

// parentDir resolves the directory that must contain the last component of
// parts. Callers must hold r.mu.
func (r *fileRegistry) parentDir(parts []string) (*namespaceNode, error) {
	parent, err := r.walk(parts[:len(parts)-1])
	if err != nil {
		return nil, err
	}
	if !parent.isDir {
		return nil, status.Errorf(codes.FailedPrecondition, "%q is not a directory", joinPath(parts[:len(parts)-1]))
	}
	return parent, nil
}

// insert adds node under the parent of path, which must already exist as a directory.
func (r *fileRegistry) insert(path string, node *namespaceNode) error {
	parts := splitPath(path)
	if len(parts) == 0 {
		return status.Error(codes.AlreadyExists, "\"/\" already exists")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	parent, err := r.parentDir(parts)
	if err != nil {
		return err
	}
	name := parts[len(parts)-1]
	if _, exists := parent.children[name]; exists {
		return status.Errorf(codes.AlreadyExists, "%q already exists", path)
	}
	parent.children[name] = node
	return nil
}

// createFile stores metadata at metadata.Path. The parent directory must exist.
func (r *fileRegistry) createFile(metadata *cnpb.FileMetadata) error {
	return r.insert(metadata.Path, &namespaceNode{metadata: metadata})
}

// mkdir creates an empty directory. The parent directory must exist.
func (r *fileRegistry) mkdir(path string) error {
	return r.insert(path, newDirNode())
}

// remove detaches the entry at path after checking it with validate.
func (r *fileRegistry) remove(path string, validate func(*namespaceNode) error) error {
	parts := splitPath(path)
	if len(parts) == 0 {
		return status.Error(codes.InvalidArgument, "the root directory cannot be removed")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	parent, err := r.parentDir(parts)
	if err != nil {
		return err
	}
	name := parts[len(parts)-1]
	node, ok := parent.children[name]
	if !ok {
		return status.Errorf(codes.NotFound, "%q does not exist", path)
	}
	if err := validate(node); err != nil {
		return err
	}
	delete(parent.children, name)
	return nil
}

// rmdir removes an empty directory.
func (r *fileRegistry) rmdir(path string) error {
	return r.remove(path, func(node *namespaceNode) error {
		if !node.isDir {
			return status.Errorf(codes.FailedPrecondition, "%q is not a directory", path)
		}
		if len(node.children) > 0 {
			return status.Errorf(codes.FailedPrecondition, "directory %q is not empty", path)
		}
		return nil
	})
}

// deleteFile removes a file entry from the namespace.
func (r *fileRegistry) deleteFile(path string) error {
	return r.remove(path, func(node *namespaceNode) error {
		if node.isDir {
			return status.Errorf(codes.FailedPrecondition, "%q is a directory", path)
		}
		return nil
	})
}

// get returns a copy of the metadata stored for the file at path.
func (r *fileRegistry) get(path string) (*cnpb.FileMetadata, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	node, err := r.walk(splitPath(path))
	if err != nil {
		return nil, err
	}
	if node.isDir {
		return nil, status.Errorf(codes.FailedPrecondition, "%q is a directory", path)
	}
	return proto.Clone(node.metadata).(*cnpb.FileMetadata), nil
}

// list returns the entries of the directory at path, sorted by name.
func (r *fileRegistry) list(path string) ([]*cnpb.DirectoryEntry, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	dir, err := r.walk(splitPath(path))
	if err != nil {
		return nil, err
	}
	if !dir.isDir {
		return nil, status.Errorf(codes.FailedPrecondition, "%q is not a directory", path)
	}

	names := make([]string, 0, len(dir.children))
	for name := range dir.children {
		names = append(names, name)
	}
	sort.Strings(names)

	entries := make([]*cnpb.DirectoryEntry, 0, len(names))
	for _, name := range names {
		child := dir.children[name]
		entry := &cnpb.DirectoryEntry{Name: name, IsDirectory: child.isDir, CreatedAtUnix: child.createdAt}
		if !child.isDir {
			entry.Size = child.metadata.Size
			entry.CreatedAtUnix = child.metadata.CreatedAtUnix
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// forEachFile visits every file below node. Callers must hold r.mu.
func forEachFile(node *namespaceNode, visit func(*cnpb.FileMetadata)) {
	for _, child := range node.children {
		if child.isDir {
			forEachFile(child, visit)
		} else {
			visit(child.metadata)
		}
	}
}

func (r *fileRegistry) addReplica(filePath string, chunkID string, nodeAddress string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	node, err := r.walk(splitPath(filePath))
	if err != nil || node.isDir {
		return
	}
	for _, chunk := range node.metadata.Chunks {
		if chunk.ChunkId == chunkID {
			chunk.NodeAddresses = append(chunk.NodeAddresses, nodeAddress)
		}
	}
}

// controlNodeServer implements the file-saving, node management and file metadata services.
type controlNodeServer struct {
	pb.UnimplementedFileSavingServer
	cnpb.UnimplementedNodeManagementServiceServer
	cnpb.UnimplementedFileMetadataServiceServer
	registry *nodeRegistry
	files    *fileRegistry
}

// allocateReplicas picks the replica DataNodes of each chunk from the active
// nodes, spreading chunks round-robin. It returns one address list per chunk.
func allocateReplicas(activeIPs []string, chunkNumber int) [][]string {
	nodeNumber := len(activeIPs)
	replicas := replicasPerChunk
	if nodeNumber < replicas {
		replicas = nodeNumber
	}

	allocations := make([][]string, 0, chunkNumber)
	for i := 0; i < chunkNumber; i++ {
		chunkIPs := make([]string, 0, replicas)
		for r := 0; r < replicas; r++ {
			chunkIPs = append(chunkIPs, activeIPs[(i+r)%nodeNumber])
		}
		allocations = append(allocations, chunkIPs)
	}
	return allocations
}

func (s *controlNodeServer) RequestNodeIPs(
	ctx context.Context,
	req *pb.RequestNodeIPsRequest,
) (*pb.RequestNodeIPsResponse, error) {
	log.Printf("Received allocation request for file %q (%d chunks)", req.FileName, req.ChunkNumber)

	filePath, err := cleanFilePath(req.FileName)
	if err != nil {
		return nil, err
	}

	activeIPs := s.registry.activeAddresses()
	nodeNumber := len(activeIPs)
	if nodeNumber == 0 {
		return nil, status.Error(codes.Unavailable, "no active DataNodes available")
	}

	allocations := allocateReplicas(activeIPs, int(req.ChunkNumber))
	IPAddress := make([]*pb.ReplicaSet, 0, len(allocations))
	for _, chunkIPs := range allocations {
		IPAddress = append(IPAddress, &pb.ReplicaSet{NodeIps: chunkIPs})
	}

	// Inside RequestNodeIPs after creating allocations:
	chunks := make([]*cnpb.ChunkAllocation, 0, len(allocations))
	for chunkIndex, chunkIPs := range allocations {
		chunks = append(chunks, &cnpb.ChunkAllocation{
			ChunkId:       fmt.Sprintf("%s_chunk_%d", req.FileName, chunkIndex),
			ChunkIndex:    int32(chunkIndex),
			NodeAddresses: chunkIPs,
		})
	}

	err = s.files.createFile(&cnpb.FileMetadata{
		Path:          filePath,
		TotalChunks:   req.ChunkNumber,
		CreatedAtUnix: time.Now().Unix(),
		Chunks:        chunks,
	})
	if err != nil {
		return nil, err
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

// cleanFilePath normalizes a client-supplied path and rejects empty or root paths.
func cleanFilePath(p string) (string, error) {
	if p == "" {
		return "", status.Error(codes.InvalidArgument, "path is required")
	}
	cleaned := pathpkg.Clean("/" + p)
	if cleaned == "/" {
		return "", status.Error(codes.InvalidArgument, "path must name a file")
	}
	return cleaned, nil
}

// cleanDirPath normalizes a directory path; an empty path means the root.
func cleanDirPath(p string) string {
	return pathpkg.Clean("/" + p)
}

func (s *controlNodeServer) CreateFileEntry(
	ctx context.Context,
	req *cnpb.CreateFileEntryRequest,
) (*cnpb.CreateFileEntryResponse, error) {
	filePath, err := cleanFilePath(req.Path)
	if err != nil {
		return nil, err
	}
	if req.Size < 0 {
		return nil, status.Error(codes.InvalidArgument, "size must not be negative")
	}
	if req.ChunkSize <= 0 {
		return nil, status.Error(codes.InvalidArgument, "chunk_size must be positive")
	}
	chunkSize := int64(req.ChunkSize)
	expectedChunks := (req.Size + chunkSize - 1) / chunkSize
	if int64(req.TotalChunks) != expectedChunks {
		return nil, status.Errorf(codes.InvalidArgument,
			"total_chunks is %d but a %d-byte file with %d-byte chunks has %d",
			req.TotalChunks, req.Size, req.ChunkSize, expectedChunks)
	}

	activeIPs := s.registry.activeAddresses()
	if len(activeIPs) == 0 {
		return nil, status.Error(codes.Unavailable, "no active DataNodes available")
	}

	allocations := allocateReplicas(activeIPs, int(req.TotalChunks))
	chunks := make([]*cnpb.ChunkAllocation, 0, len(allocations))
	for i, nodeAddresses := range allocations {
		size := chunkSize
		if remaining := req.Size - int64(i)*chunkSize; remaining < size {
			size = remaining
		}
		chunks = append(chunks, &cnpb.ChunkAllocation{
			ChunkId:       fmt.Sprintf("%s_chunk_%d", filePath, i),
			ChunkIndex:    int32(i),
			ChunkSize:     size,
			NodeAddresses: nodeAddresses,
		})
	}

	metadata := &cnpb.FileMetadata{
		Path:          filePath,
		Size:          req.Size,
		TotalChunks:   req.TotalChunks,
		CreatedAtUnix: time.Now().Unix(),
		Chunks:        chunks,
	}
	// Reply with a copy so the stored metadata is never shared with the gRPC layer.
	responseChunks := proto.Clone(metadata).(*cnpb.FileMetadata).Chunks
	if err := s.files.createFile(metadata); err != nil {
		return nil, err
	}
	log.Printf("Registered file %q (%d bytes, %d chunks)", filePath, req.Size, req.TotalChunks)

	return &cnpb.CreateFileEntryResponse{Path: filePath, Chunks: responseChunks}, nil
}

func (s *controlNodeServer) GetFileMetadata(
	ctx context.Context,
	req *cnpb.GetFileMetadataRequest,
) (*cnpb.GetFileMetadataResponse, error) {
	filePath, err := cleanFilePath(req.Path)
	if err != nil {
		return nil, err
	}

	metadata, err := s.files.get(filePath)
	if err != nil {
		return nil, err
	}
	return &cnpb.GetFileMetadataResponse{Metadata: metadata}, nil
}

func (s *controlNodeServer) ListDirectory(
	ctx context.Context,
	req *cnpb.ListDirectoryRequest,
) (*cnpb.ListDirectoryResponse, error) {
	dirPath := cleanDirPath(req.Path)

	entries, err := s.files.list(dirPath)
	if err != nil {
		return nil, err
	}
	return &cnpb.ListDirectoryResponse{Path: dirPath, Entries: entries}, nil
}

func (s *controlNodeServer) DeleteFile(
	ctx context.Context,
	req *cnpb.DeleteFileRequest,
) (*cnpb.DeleteFileResponse, error) {
	filePath, err := cleanFilePath(req.Path)
	if err != nil {
		return nil, err
	}

	if err := s.files.deleteFile(filePath); err != nil {
		return nil, err
	}
	log.Printf("Deleted file %q", filePath)
	return &cnpb.DeleteFileResponse{Deleted: true}, nil
}

func subtractSlices(source []string, toExclude []string) []string {
	excludedLookup := make(map[string]bool, len(toExclude))
	for _, item := range toExclude {
		excludedLookup[item] = true
	}
	var difference []string
	for _, item := range source {
		if !excludedLookup[item] {
			difference = append(difference, item)
		}
	}
	return difference
}

func intersectSlices(first []string, second []string) []string {
	lookup := make(map[string]bool, len(second))
	for _, item := range second {
		lookup[item] = true
	}
	var common []string
	for _, item := range first {
		if lookup[item] {
			common = append(common, item)
		}
	}
	return common
}

func (registry *fileRegistry) findUnderReplicatedChunks(
	activeNodes []string,
	targetReplicationFactor int,
) []replicationJob {
	registry.mu.Lock()
	defer registry.mu.Unlock()

	var pendingJobs []replicationJob

	forEachFile(registry.root, func(fileMetadata *cnpb.FileMetadata) {
		for _, fileChunk := range fileMetadata.Chunks {
			nodeAddresses := fileChunk.NodeAddresses
			healthyReplicas := intersectSlices(activeNodes, nodeAddresses)

			if len(healthyReplicas) == 0 || len(healthyReplicas) >= targetReplicationFactor {
				continue
			}

			candidateReplicas := subtractSlices(activeNodes, healthyReplicas)
			if len(candidateReplicas) == 0 {
				continue
			}

			chunkReplicationJob := replicationJob{
				filePath:              fileMetadata.Path,
				chunkID:               fileChunk.ChunkId,
				sourceDataNodeAddress: healthyReplicas[0],
				targetDataNodeAddress: candidateReplicas[0],
			}

			pendingJobs = append(pendingJobs, chunkReplicationJob)
		}
	})
	return pendingJobs
}

func executeReplicationJob(ctx context.Context, job replicationJob) error {
	replicationctx, replicationCancel := context.WithTimeout(ctx, 10*time.Second)
	defer replicationCancel()

	replicationClientConn, err := grpc.NewClient(job.sourceDataNodeAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("connecting to client %v %w", job.sourceDataNodeAddress, err)
	}
	defer replicationClientConn.Close()

	datanodeClient := dnpb.NewDataNodeServiceClient(replicationClientConn)

	getChunkResponse, err := datanodeClient.GetChunk(replicationctx, &dnpb.GetChunkRequest{
		ChunkId: job.chunkID,
	})
	if err != nil {
		return fmt.Errorf("fetching chunk %q from source %q: %w", job.chunkID, job.sourceDataNodeAddress, err)
	}

	replicationClientConn, err = grpc.NewClient(job.targetDataNodeAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("connecting to client %v %w", job.targetDataNodeAddress, err)
	}
	defer replicationClientConn.Close()

	datanodeClient = dnpb.NewDataNodeServiceClient(replicationClientConn)

	storeChunkRequest := &dnpb.StoreChunkRequest{
		ChunkId:        job.chunkID,
		ChunkPayload:   getChunkResponse.ChunkPayload,
		CheckSum:       getChunkResponse.CheckSum,
		ReplicaTargets: nil}

	storeChunkResponse, err := datanodeClient.StoreChunk(replicationctx, storeChunkRequest)
	if err != nil {
		return fmt.Errorf("storing chunk %q to %q: %w", job.chunkID, job.targetDataNodeAddress, err)
	}
	if !storeChunkResponse.WriteSuccessful {
		return fmt.Errorf("writing chunk %q to %q: %w", job.chunkID, job.targetDataNodeAddress, err)
	}
	// TODO: Step 5 - Return storytelling error on failure or nil on success

	return nil
}
