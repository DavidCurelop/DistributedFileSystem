package main

import (
	"context"
	"crypto/sha256"
	pb "distributedFileSystem/pkg/proto/datanode"
	"encoding/hex"
	"flag"
	"log"
	"net"
	"os"
	"path/filepath"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
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

	return &pb.StoreChunkResponse{ChunkId: req.ChunkId, WriteSuccessful: true}, nil
}
