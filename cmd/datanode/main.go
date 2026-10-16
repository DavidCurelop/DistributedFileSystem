package main

import (
	"context"
	"crypto/sha256"
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

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func main() {
	port := flag.String("port", ":50052", "DataNode gRPC listening port")
	dataDir := flag.String("data-dir", "./data/chunks", "Path to chunk storage directory")
	flag.Parse()

	listener, err := net.Listen("tcp", *port)
	if err != nil {
		log.Fatalf("listening on tcp %s: %v", *port, err)
	}

	datanodeServer := grpc.NewServer()
	pb.RegisterDataNodeServiceServer(datanodeServer, &dataNodeServer{storageDirectory: *dataDir})
	log.Printf("DataNode listening on %s, saving to %s", *port, *dataDir)

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
		return fmt.Errorf("dialing transport connection to %q: %w", heartbeatWorker.controlNodeAddress, err)
	}
	heartbeatClient := cnpb.NewNodeManagementServiceClient(heartbeatClientconn)
	// TODO: Step 3 - Execute initial registration to acquire node ID and heartbeat interval

	// TODO: Step 4 - Launch background loop handling ticker events and context cancellation
	return nil
}

func (heartbeatWorker *heartbeatWorker) sendHeartbeat(ctx context.Context) error {
	// TODO: Step 5 - Create context.WithTimeout for outbound RPC
	// TODO: Step 6 - Gather disk metrics and invoke w.client.Heartbeat
	// TODO: Step 7 - If response indicates ReregisterRequired, trigger re-registration flow
	return nil
}

func (heartbeatWorker *heartbeatWorker) scanStoredChunks() ([]string, error) {
	// TODO: Step 8 - Inspect storageDirectory and gather list of active chunk IDs
	return nil
}
