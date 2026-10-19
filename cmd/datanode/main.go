package main

import (
	"context"
	"crypto/sha256"
	"distributedFileSystem/pkg/config"
	cnpb "distributedFileSystem/pkg/proto/controlnode"
	pb "distributedFileSystem/pkg/proto/datanode"
	"distributedFileSystem/pkg/security"
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
	"google.golang.org/grpc/status"
)

const registrationRetryInterval = 3 * time.Second

func main() {
	portFlag := flag.String("port", config.EnvString("PORT", ":50052"), "DataNode gRPC listening port or address (env PORT)")
	controlAddrFlag := flag.String("ctrlAdr", config.EnvString("CONTROL_ADDR", "localhost:50051"), "Comma-separated ControlNode addresses, host[:port]; heartbeats are sent to every one (env CONTROL_ADDR)")
	nodeAddrFlag := flag.String("nodeAdr", config.EnvString("NODE_ADDR", "localhost"), "Address of this data node as seen by the cluster, host[:port] (env NODE_ADDR)")
	dataDirFlag := flag.String("data-dir", config.EnvString("DATA_DIR", "./data/chunks"), "Path to chunk storage directory (env DATA_DIR)")
	authToken := flag.String("auth-token", config.EnvString("AUTH_TOKEN", security.DefaultAuthToken), "Shared token required from every caller and sent to other nodes (env AUTH_TOKEN)")
	tlsCert := flag.String("tls-cert", config.EnvString("TLS_CERT_FILE", ""), "TLS certificate PEM file; empty generates a self-signed one (env TLS_CERT_FILE)")
	tlsKey := flag.String("tls-key", config.EnvString("TLS_KEY_FILE", ""), "TLS private key PEM file (env TLS_KEY_FILE)")
	tlsCA := flag.String("tls-ca", config.EnvString("TLS_CA_FILE", ""), "CA PEM file used to verify peer certificates; empty skips verification (env TLS_CA_FILE)")
	flag.Parse()

	port := config.ListenAddr(*portFlag)
	controlNodeAddresses := config.AddressList(*controlAddrFlag, "50051")
	if len(controlNodeAddresses) == 0 {
		log.Fatalf("at least one ControlNode address is required")
	}
	nodeAddr := config.WithDefaultPort(*nodeAddrFlag, config.Port(port))
	dataDir := dataDirFlag

	if err := security.ValidateToken(*authToken); err != nil {
		log.Fatalf("invalid auth token: %v", err)
	}
	nodeHost, _, _ := net.SplitHostPort(nodeAddr)
	serverOptions, err := security.ServerOptions(*tlsCert, *tlsKey, *authToken, nodeHost)
	if err != nil {
		log.Fatalf("configuring TLS and authentication: %v", err)
	}
	dialOptions, err := security.ClientDialOptions(*tlsCA, *authToken)
	if err != nil {
		log.Fatalf("configuring outbound TLS: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, controlNodeAddress := range controlNodeAddresses {
		worker := newHeartbeatWorker(controlNodeAddress, nodeAddr, *dataDir, dialOptions)
		go func() {
			err := worker.Start(ctx)
			if err != nil && !errors.Is(err, context.Canceled) {
				log.Fatalf("heartbeat worker for ControlNode %q stopped: %v", controlNodeAddress, err)
			}
		}()
	}
	listener, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("listening on tcp %s: %v", port, err)
	}

	datanodeServer := grpc.NewServer(serverOptions...)
	pb.RegisterDataNodeServiceServer(datanodeServer, &dataNodeServer{storageDirectory: *dataDir, dialOptions: dialOptions})
	log.Printf("DataNode listening on %s (advertised as %s), saving to %s", port, nodeAddr, *dataDir)

	if err := datanodeServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve traffic: %v", err)
	}
}

type dataNodeServer struct {
	pb.UnimplementedDataNodeServiceServer
	storageDirectory string
	dialOptions      []grpc.DialOption
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

func (s *dataNodeServer) forwardToNextReplica(
	ctx context.Context,
	downstreamAddress string,
	req *pb.StoreChunkRequest,
	remainingTargets []string,
) error {
	dataNodeConnection, err := grpc.NewClient(downstreamAddress, s.dialOptions...)
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
	dialOptions        []grpc.DialOption
	client             cnpb.NodeManagementServiceClient
}

func newHeartbeatWorker(controlAddr, dataNodeAddr, storageDir string, dialOptions []grpc.DialOption) *heartbeatWorker {
	heartbeatWorker := &heartbeatWorker{
		controlNodeAddress: controlAddr,
		dataNodeAddress:    dataNodeAddr,
		storageDirectory:   storageDir,
		dialOptions:        dialOptions,
	}
	return heartbeatWorker
}

func (heartbeatWorker *heartbeatWorker) Start(ctx context.Context) error {
	heartbeatClientconn, err := grpc.NewClient(heartbeatWorker.controlNodeAddress, heartbeatWorker.dialOptions...)
	if err != nil {
		return fmt.Errorf("dialing transport connection to %q: %v", heartbeatWorker.controlNodeAddress, err)
	}
	defer heartbeatClientconn.Close()
	heartbeatWorker.client = cnpb.NewNodeManagementServiceClient(heartbeatClientconn)
	registerNodeResponse, err := heartbeatWorker.registerUntilAccepted(ctx)
	if err != nil {
		return fmt.Errorf("failed to register datanode %v %v", heartbeatWorker.dataNodeAddress, err)
	}

	heartbeatWorker.assignedNodeID = registerNodeResponse.NodeId
	heartbeatIntervalSeconds := time.Duration(registerNodeResponse.HeartbeatIntervalSeconds) * time.Second

	heartbeatTicker := time.NewTicker(heartbeatIntervalSeconds)
	defer heartbeatTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-heartbeatTicker.C:
			err = heartbeatWorker.sendHeartbeat(ctx)
			if err != nil {
				log.Printf("sending heartbeat from %v to ControlNode %q: %v", heartbeatWorker.assignedNodeID, heartbeatWorker.controlNodeAddress, err)
			}
		}
	}
}

func (heartbeatWorker *heartbeatWorker) registerUntilAccepted(ctx context.Context) (*cnpb.RegisterNodeResponse, error) {
	registrationRetryTicker := time.NewTicker(registrationRetryInterval)
	defer registrationRetryTicker.Stop()

	for {
		registerNodeResponse, err := heartbeatWorker.registerNode(ctx)
		if err == nil {
			return registerNodeResponse, nil
		}
		log.Printf("registering with ControlNode %q (retrying in %v): %v", heartbeatWorker.controlNodeAddress, registrationRetryInterval, err)

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-registrationRetryTicker.C:
		}
	}
}

func (heartbeatWorker *heartbeatWorker) registerNode(ctx context.Context) (*cnpb.RegisterNodeResponse, error) {
	totalDisk, availableDisk := heartbeatWorker.diskMetrics()
	registerNodeRequest := &cnpb.RegisterNodeRequest{
		NodeAddress:        heartbeatWorker.dataNodeAddress,
		TotalDiskBytes:     totalDisk,
		AvailableDiskBytes: availableDisk,
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
	heartbeatctx, heartbeatctxCancel := context.WithTimeout(ctx, 5*time.Second)
	defer heartbeatctxCancel()
	storedChunks, err := heartbeatWorker.scanStoredChunks()
	if err != nil {
		return fmt.Errorf("gathering stored chunks for %v, %w", heartbeatWorker.assignedNodeID, err)
	}
	_, availableDisk := heartbeatWorker.diskMetrics()
	heartbeatRequest := &cnpb.HeartbeatRequest{
		NodeId:             heartbeatWorker.assignedNodeID,
		AvailableDiskBytes: availableDisk,
		StoredChunkCount:   int32(len(storedChunks)),
		TimestampUnix:      time.Now().Unix(),
	}

	heartbeatResponse, err := heartbeatWorker.client.Heartbeat(heartbeatctx, heartbeatRequest)
	if err != nil {
		return fmt.Errorf("sending heartbeat from %v %w", heartbeatWorker.assignedNodeID, err)
	}
	if heartbeatResponse.ReregisterRequired {
		registerNodeResponse, err := heartbeatWorker.registerNode(ctx)
		if err != nil {
			return fmt.Errorf("re-registering target %v %w", heartbeatWorker.assignedNodeID, err)
		}
		heartbeatWorker.assignedNodeID = registerNodeResponse.NodeId
	}
	return nil
}

func (heartbeatWorker *heartbeatWorker) diskMetrics() (total, available int64) {
	total, available, err := diskUsage(heartbeatWorker.storageDirectory)
	if err != nil {
		log.Printf("gathering disk metrics for %v: %v", heartbeatWorker.storageDirectory, err)
		return 0, 0
	}
	return total, available
}

func (heartbeatWorker *heartbeatWorker) scanStoredChunks() ([]string, error) {
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
