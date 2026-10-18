package main

import (
	"context"
	"crypto/sha256"
	"distributedFileSystem/pkg/config"
	cnpb "distributedFileSystem/pkg/proto/controlnode"
	pb "distributedFileSystem/pkg/proto/datanode"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func main() {
	portFlag := flag.String("port", config.EnvString("PORT", ":50052"), "DataNode gRPC listening port or address (env PORT)")
	controlAddrFlag := flag.String("ctrlAdr", config.EnvString("CONTROL_ADDR", "localhost:50051"), "Address of the control node, host[:port] (env CONTROL_ADDR)")
	nodeAddrFlag := flag.String("nodeAdr", config.EnvString("NODE_ADDR", "localhost"), "Address of this data node as seen by the cluster, host[:port] (env NODE_ADDR)")
	dataDirFlag := flag.String("data-dir", config.EnvString("DATA_DIR", "./data/chunks"), "Path to chunk storage directory (env DATA_DIR)")
	flag.Parse()

	port := config.ListenAddr(*portFlag)
	controlAddr := config.WithDefaultPort(*controlAddrFlag, "50051")
	nodeAddr := config.WithDefaultPort(*nodeAddrFlag, config.Port(port))
	dataDir := dataDirFlag

	worker := newHeartbeatWorker(controlAddr, nodeAddr, *dataDir)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		err := worker.Start(ctx)
		if err != nil {
			log.Fatalf("starting heartbeat worker %v", err)
		}
	}()
	listener, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("listening on tcp %s: %v", port, err)
	}

	datanodeServer := grpc.NewServer()
	pb.RegisterDataNodeServiceServer(datanodeServer, &dataNodeServer{storageDirectory: *dataDir})
	log.Printf("DataNode listening on %s (advertised as %s), saving to %s", port, nodeAddr, *dataDir)

	if err := datanodeServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve traffic: %v", err)
	}
}

type dataNodeServer struct {
	pb.UnimplementedDataNodeServiceServer
	storageDirectory string
}

func (s *dataNodeServer) StoreChunk(ctx context.Context, req *pb.StoreChunkRequest) (*pb.StoreChunkResponse, error) {
	if req.ChunkId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "Chunk Id must not be nil")
	}

	if len(req.ChunkPayload) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "Chunk Payload must not be nil")
	}

	computeHash := sha256.Sum256(req.ChunkPayload)
	hashString := hex.EncodeToString(computeHash[:])
	if hashString != req.CheckSum {
		return nil, status.Errorf(codes.InvalidArgument, "SHA does not match payload")
	}

	err := os.MkdirAll(s.storageDirectory, 0755)
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "Could not create directory %v, %v", s.storageDirectory, err)
	}

	targetPath := filepath.Join(s.storageDirectory, req.ChunkId+".bin")
	err = os.WriteFile(targetPath, req.ChunkPayload, 0644)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "writing chunk %q to disk: %v", req.ChunkId, err)
	}

	checksumFilePath := filepath.Join(s.storageDirectory, req.ChunkId+".checksum")
	err = os.WriteFile(checksumFilePath, []byte(hashString), 0644)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "writing checksum %q to disk: %v", req.ChunkId, err)
	}

	if len(req.ReplicaTargets) > 0 {
		err = s.forwardToNextReplica(ctx, req.ReplicaTargets[0], req, req.ReplicaTargets[1:])

		if err != nil {
			return nil, status.Errorf(codes.Internal, "replicating to next target %q %v", req.ReplicaTargets[0], err)
		}
	}

	return &pb.StoreChunkResponse{ChunkId: req.ChunkId, WriteSuccessful: true}, nil
}

// Structural Scaffolding for Downstream Forwarding
func (s *dataNodeServer) forwardToNextReplica(
	ctx context.Context,
	downstreamAddress string,
	req *pb.StoreChunkRequest,
	remainingTargets []string,
) error {
	dataNodeConnection, err := grpc.NewClient(downstreamAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("connecting to server %q: %w", downstreamAddress, err)
	}

	defer dataNodeConnection.Close()

	replicaClient := pb.NewDataNodeServiceClient(dataNodeConnection)
	replicaStoreRequest := &pb.StoreChunkRequest{
		ChunkId:        req.ChunkId,
		ChunkPayload:   req.ChunkPayload,
		CheckSum:       req.CheckSum,
		ReplicaTargets: remainingTargets,
	}

	storeChunkResponse, err := replicaClient.StoreChunk(ctx, replicaStoreRequest)

	if err != nil {
		return fmt.Errorf("forwarding chunk %q to %q: %w", req.ChunkId, downstreamAddress, err)
	}

	if !storeChunkResponse.WriteSuccessful {
		return fmt.Errorf("downstream replica %q reported write failure for chunk %q", downstreamAddress, req.ChunkId)
	}

	return nil
}

// GetChunk retrieves chunk data and verifies at-rest integrity before returning.
func (s *dataNodeServer) GetChunk(
	ctx context.Context,
	req *pb.GetChunkRequest,
) (*pb.GetChunkResponse, error) {
	if req.ChunkId == "" {
		return nil, status.Error(codes.InvalidArgument, "chunk id cant be empty")
	}

	targetPath := filepath.Join(s.storageDirectory, req.ChunkId+".bin")
	checksumFilePath := filepath.Join(s.storageDirectory, req.ChunkId+".checksum")

	chunkPayload, err := os.ReadFile(targetPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, status.Errorf(codes.NotFound, "chunk file does not exist at %q: %v", targetPath, err)
		}
		if errors.Is(err, os.ErrPermission) {
			return nil, status.Errorf(codes.PermissionDenied, "permission denied reading chunk at %q: %v", targetPath, err)
		}
		return nil, status.Errorf(codes.Internal, "unexpected error reading chunk at %q: %v", targetPath, err)
	}

	checksumPayload, err := os.ReadFile(checksumFilePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, status.Errorf(codes.NotFound, "checksum file does not exist at %q: %v", checksumFilePath, err)
		}
		return nil, status.Errorf(codes.Internal, "unexpected error reading checksum at %q: %v", checksumFilePath, err)
	}

	computeHash := sha256.Sum256(chunkPayload)
	hashString := hex.EncodeToString(computeHash[:])
	if hashString != strings.TrimSpace(string(checksumPayload)) {
		return nil, status.Errorf(codes.DataLoss, "SHA does not match payload")
	}

	return &pb.GetChunkResponse{ChunkId: req.ChunkId, ChunkPayload: chunkPayload, CheckSum: hashString}, nil
}

type heartbeatWorker struct {
	controlNodeAddress string
	dataNodeAddress    string
	storageDirectory   string
	assignedNodeID     string
	client             cnpb.NodeManagementServiceClient
}

func newHeartbeatWorker(controlAddr, dataNodeAddr, storageDir string) *heartbeatWorker {
	// TODO: Step 1 - Initialize the worker struct
	heartbeatWorker := &heartbeatWorker{
		controlNodeAddress: controlAddr,
		dataNodeAddress:    dataNodeAddr,
		storageDirectory:   storageDir,
	}
	return heartbeatWorker
}

func (heartbeatWorker *heartbeatWorker) Start(ctx context.Context) error {
	// TODO: Step 2 - Establish gRPC connection using grpc.NewClient
	heartbeatClientconn, err := grpc.NewClient(heartbeatWorker.controlNodeAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dialing transport connection to %q: %v", heartbeatWorker.controlNodeAddress, err)
	}
	heartbeatWorker.client = cnpb.NewNodeManagementServiceClient(heartbeatClientconn)
	// TODO: Step 3 - Execute initial registration to acquire node ID and heartbeat interval
	registerNodeResponse, err := heartbeatWorker.registerNode(ctx)
	if err != nil {
		return fmt.Errorf("failed to register datanode %v %v", heartbeatWorker.dataNodeAddress, err)
	}

	heartbeatWorker.assignedNodeID = registerNodeResponse.NodeId
	heartbeatIntervalSeconds := time.Duration(registerNodeResponse.HeartbeatIntervalSeconds) * time.Second

	heartbeatTicker := time.NewTicker(heartbeatIntervalSeconds)
	defer heartbeatTicker.Stop()
	// TODO: Step 4 - Launch background loop handling ticker events and context cancellation
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-heartbeatTicker.C:
			err = heartbeatWorker.sendHeartbeat(ctx)
			if err != nil {
				log.Printf("sending heartbeat from %v %v", heartbeatWorker.assignedNodeID, err)
			}
		}
	}
}

func (heartbeatWorker *heartbeatWorker) registerNode(ctx context.Context) (*cnpb.RegisterNodeResponse, error) {
	registerNodeRequest := &cnpb.RegisterNodeRequest{
		NodeAddress: heartbeatWorker.dataNodeAddress,
	}
	registerNodectx, registerNodeCancel := context.WithTimeout(ctx, 5*time.Second)
	defer registerNodeCancel()

	registerNodeResponse, err := heartbeatWorker.client.RegisterNode(registerNodectx, registerNodeRequest)
	if err != nil {
		return nil, fmt.Errorf("failed to register datanode %v %w", heartbeatWorker.dataNodeAddress, err)
	}
	return registerNodeResponse, nil
}

func (heartbeatWorker *heartbeatWorker) sendHeartbeat(ctx context.Context) error {
	// TODO: Step 5 - Create context.WithTimeout for outbound RPC
	heartbeatctx, heartbeatctxCancel := context.WithTimeout(ctx, 5*time.Second)
	defer heartbeatctxCancel()
	// TODO: Step 6 - Gather disk metrics and invoke w.client.Heartbeat
	storedChunks, err := heartbeatWorker.scanStoredChunks()
	if err != nil {
		return fmt.Errorf("gathering stored chunks for %v, %w", heartbeatWorker.assignedNodeID, err)
	}
	heartbeatRequest := &cnpb.HeartbeatRequest{
		StoredChunkCount: int32(len(storedChunks)),
		NodeId:           heartbeatWorker.assignedNodeID,
	}

	heartbeatResponse, err := heartbeatWorker.client.Heartbeat(heartbeatctx, heartbeatRequest)
	if err != nil {
		return fmt.Errorf("sending heartbeat from %v %w", heartbeatWorker.assignedNodeID, err)
	}
	// TODO: Step 7 - If response indicates ReregisterRequired, trigger re-registration flow
	if heartbeatResponse.ReregisterRequired {
		registerNodeResponse, err := heartbeatWorker.registerNode(ctx)
		if err != nil {
			return fmt.Errorf("re-registering target %v %w", heartbeatWorker.assignedNodeID, err)
		}
		heartbeatWorker.assignedNodeID = registerNodeResponse.NodeId
	}
	return nil
}

func (heartbeatWorker *heartbeatWorker) scanStoredChunks() ([]string, error) {
	// TODO: Step 8 - Inspect storageDirectory and gather list of active chunk IDs
	storageDir := heartbeatWorker.storageDirectory
	directory, err := os.ReadDir(storageDir)
	var storedChunks []string
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return storedChunks, nil
		} else {
			return nil, fmt.Errorf("getting stored chunks from %v %w", storageDir, err)
		}
	}
	for _, file := range directory {
		if strings.HasSuffix(file.Name(), ".bin") {
			storedChunks = append(storedChunks, strings.TrimSuffix(file.Name(), ".bin"))
		}
	}

	return storedChunks, nil
}
