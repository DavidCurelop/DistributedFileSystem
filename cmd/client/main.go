package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	pb "distributedFileSystem/pkg/proto/client"
	datanodePb "distributedFileSystem/pkg/proto/datanode"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const defaultPartitionSize = 4 * 1024 * 1024

func main() {
	chunks, err := partition("G:/My Drive/Eafit/Semester9/CloudComputing/Lab2/Lab2Vid.mp4")
	if err != nil {
		log.Fatalf("Partition failed: %v", err)
	}
	log.Printf("Successfully created %d partition(s)!", len(chunks))

	controlNodeIP := "localhost:50051"
	requestPayload := &pb.RequestNodeIPsRequest{
		ChunkNumber: int32(len(chunks)),
		FileName:    "Lab2Vid.mp4",
	}
	filePartitionAddresses, err := getPartitionAddresses(controlNodeIP, requestPayload)
	if err != nil {
		log.Fatalf("getting partition addresses: %v", err)
	}

	for _, replicaIP := range filePartitionAddresses.IpAddress {
		log.Printf("Main ip: %v, Replica IP: %v", replicaIP.NodeIps[0], replicaIP.NodeIps[1])
	}

}

func getPartitionAddresses(controlNodeIP string, requestPayload *pb.RequestNodeIPsRequest) (*pb.RequestNodeIPsResponse, error) {
	controlNodeConnection, err := grpc.NewClient(controlNodeIP, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("connecting to server %q: %w", controlNodeIP, err)
	}
	defer controlNodeConnection.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	fileSavingClient := pb.NewFileSavingClient(controlNodeConnection)

	response, err := fileSavingClient.RequestNodeIPs(ctx, requestPayload)
	if err != nil {
		return nil, fmt.Errorf("requesting node IPs: %w", err)
	}
	return response, nil
}

func partition(sourceFilePath string) ([][]byte, error) {
	var chunks [][]byte
	sourceFile, err := os.Open(sourceFilePath)
	if err != nil {
		return nil, fmt.Errorf("opening source file %q: %w", sourceFilePath, err)
	}
	defer sourceFile.Close()
	partitionIndex := 0
	for {
		partitionBuffer := make([]byte, defaultPartitionSize)
		bytesRead, err := io.ReadFull(sourceFile, partitionBuffer)

		if err == io.EOF {
			break
		}

		if err != nil && err != io.ErrUnexpectedEOF {
			return nil, fmt.Errorf("reading partition %d: %w", partitionIndex, err)
		}

		currentChunk := partitionBuffer[:bytesRead]
		log.Printf("Partition %d ready: %d bytes", partitionIndex, len(currentChunk))
		chunks = append(chunks, currentChunk)

		partitionIndex++

		if err == io.ErrUnexpectedEOF {
			break
		}
	}
	return chunks, nil
}

func sendChunk(ctx context.Context, dataNodeIP string, replicaTargets []string, chunkID string, payload []byte, maxAttempts int) (*datanodePb.StoreChunkResponse, error) {
	computeHash := sha256.Sum256(payload)
	computeHashString := hex.EncodeToString(computeHash[:])

	dataNodeConnection, err := grpc.NewClient(dataNodeIP, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("connecting to server %q: %w", dataNodeIP, err)
	}
	defer dataNodeConnection.Close()

	storeChunkClient := datanodePb.NewDataNodeServiceClient(dataNodeConnection)

	storeChunkrequest := &datanodePb.StoreChunkRequest{
		ChunkId:        chunkID,
		ChunkPayload:   payload,
		CheckSum:       computeHashString,
		ReplicaTargets: replicaTargets,
	}

	for attempt := 0; attempt < maxAttempts; attempt++ {
		attemptCtx, cancelAttempt := context.WithTimeout(ctx, 5*time.Second)
		storeChunkResponse, err := storeChunkClient.StoreChunk(attemptCtx, storeChunkrequest)
		cancelAttempt()

		if err == nil {
			if storeChunkResponse.GetWriteSuccessful() {
				return storeChunkResponse, nil
			}
			return nil, fmt.Errorf("node %s rejected chunk %s write", dataNodeIP, chunkID)
		}

		if !isTemporary(err) || attempt == maxAttempts-1 {
			return nil, fmt.Errorf("storing chunk %s on %s (attempt %d/%d): %w", chunkID, dataNodeIP, attempt+1, maxAttempts, err)
		}

		// Context-aware exponential backoff
		backoffDelay := 100 * time.Millisecond * (1 << attempt)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoffDelay):
		}

	}

	return nil, fmt.Errorf("max number of attempts reached")
}

func isTemporary(err error) bool {
	rpcStatus, isGrpcStatus := status.FromError(err)
	if isGrpcStatus && rpcStatus.Code() == codes.Unavailable {
		return true
	}

	return false
}

func sendChunksToServer(ctx context.Context, fileName string, chunks [][]byte, partitionAddresses *pb.RequestNodeIPsResponse) error {
	// TODO: Step 1 - Validate len(chunks) == len(partitionAddresses.GetIpAddress())
	filePartitionAddresses, err := getPartitionAddresses(controlNodeIP, requestPayload)
	if err != nil {
		return fmt.Errorf("getting partition addresses: %v", err)
	}

	if len(chunks) > len(filePartitionAddresses.IpAddress){
		return fmt.Errorf("The must be less chunks than IPs")
	}
	for chunkIndex, payload := range chunks {
		chunkID := fmt.Sprintf("%s_chunk_%d", fileName, chunkIndex)
		// TODO: Step 2 - Extract ReplicaSet and guard len(replicaSet.GetNodeIps()) > 0
		// TODO: Step 3 - Primary is NodeIps[0], replicaTargets are NodeIps[1:]
		// TODO: Step 4 - Invoke sendChunk(ctx, primaryIP, replicaTargets, chunkID, payload, 3)
		// TODO: Step 5 - If err != nil, wrap and return early (Line of Sight)
	}
	return nil
}
