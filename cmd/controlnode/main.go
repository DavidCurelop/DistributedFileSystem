package main

import (
	"context"
	"log"
	"net"

	pb "distributedFileSystem/pkg/proto/client"
	"google.golang.org/grpc"
)

var availIPs = [4]string{"127.0.0.1:50052", "127.0.0.1:50053", "127.0.0.1:50054", "127.0.0.1:50055"}

// controlNodeServer implements the generated pb.FileSavingServer interface
type controlNodeServer struct {
	pb.UnimplementedFileSavingServer
	// TODO: What cluster state does the ControlNode need to store?
	// (e.g. registered DataNode addresses, file metadata map, lock?)
}

func (s *controlNodeServer) RequestNodeIPs(
	ctx context.Context,
	req *pb.RequestNodeIPsRequest,
) (*pb.RequestNodeIPsResponse, error) {
	log.Printf("Received allocation request for file %q (%d chunks)", req.FileName, req.ChunkNumber)
	nodeNumber := len(availIPs)
	chunkNumber := int(req.ChunkNumber)
	IPAddress := make([]*pb.ReplicaSet, 0, chunkNumber)
	// TODO: Step 1 - For each chunk (from 0 to req.ChunkNumber - 1), choose replica nodes
	for i := 0; i < int(chunkNumber); i++ {
		allocatedChunkNumber := i % nodeNumber
		allocatedReplicaChunkNumber := (i + 1) % nodeNumber

		chunkIPs := &pb.ReplicaSet{NodeIps: []string{availIPs[allocatedChunkNumber], availIPs[allocatedReplicaChunkNumber]}}
		IPAddress = append(IPAddress, chunkIPs)
	}

	return &pb.RequestNodeIPsResponse{IpAddress: IPAddress}, nil
}

func main() {
	port := ":50051"
	controlNodeListener, err := net.Listen("tcp", port)

	if err != nil {
		log.Fatalf("listening on tcp address %q: %v", port, err)
	}

	controlServer := grpc.NewServer()
	controlNodeServer := &controlNodeServer{}
	pb.RegisterFileSavingServer(controlServer, controlNodeServer)

	if err := controlServer.Serve(controlNodeListener); err != nil {
		log.Fatalf("Failed to serve traffic: %v", err)
	}
}
