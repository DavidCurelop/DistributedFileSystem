package main

import (
	"context"
	"crypto/sha256"
	pb "distributedFileSystem/pkg/proto/datanode"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
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
	if hex.EncodeToString(computeHash[:]) != req.CheckSum {
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
	// TODO: Step 1 - Establish gRPC client connection to downstreamAddress
	dataNodeConnection, err := grpc.NewClient(downstreamAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("connecting to server %q: %w", downstreamAddress, err)
	}

	// TODO: Step 2 - Defer connection teardown or handle client lifecycle
	defer dataNodeConnection.Close()

	replicaClient := pb.NewDataNodeServiceClient(dataNodeConnection)
	// TODO: Step 3 - Construct downstream request with updated remainingTargets
	replicaStoreRequest := &pb.StoreChunkRequest{
		ChunkId:        req.ChunkId,
		ChunkPayload:   req.ChunkPayload,
		CheckSum:       req.CheckSum,
		ReplicaTargets: remainingTargets,
	}
	storeChunkResponse, err := replicaClient.StoreChunk(ctx, replicaStoreRequest)
	// TODO: Step 4 - Execute StoreChunk RPC with propagated context
	if err != nil {
		return fmt.Errorf("forwarding chunk %q to %q: %w", req.ChunkId, downstreamAddress, err)
	}
	// TODO: Step 5 - Guard against downstream write failure and return error
	if !storeChunkResponse.WriteSuccessful {
		return fmt.Errorf("downstream replica %q reported write failure for chunk %q", downstreamAddress, req.ChunkId)
	}

	return nil
}
