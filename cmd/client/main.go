package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	pb "distributedFileSystem/pkg/proto/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const defaultPartitionSize = 4 * 1024 * 1024

func main() {
	chunks, err := partition("G:/My Drive/Eafit/Semester9/CloudComputing/Lab2/Lab2Vid.mp4")
	if err != nil {
		log.Fatalf("Partition failed: %v", err)
	}
	log.Printf("Successfully created %d partition(s)!", len(chunks))

	controlNodeIP := "localhost:50051"
	filePartitionAddresses, err := getPartitionAddresses(controlNodeIP, chunks)
	if err != nil {
		log.Fatalf("getting partition addresses: %v", err)
	}

	for _, replicaIP := range(filePartitionAddresses.IpAddress){
		log.Printf("Main ip: %v, Replica IP: %v", replicaIP.NodeIps[0], replicaIP.NodeIps[1])
	}
}

func getPartitionAddresses(controlNodeIP string, chunks [][]byte) (*pb.RequestNodeIPsResponse, error) {
	controlNodeConnection, err := grpc.NewClient(controlNodeIP, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("connecting to server %q: %w", controlNodeIP, err)
	}
	defer controlNodeConnection.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	fileSavingClient := pb.NewFileSavingClient(controlNodeConnection)
	requestPayload := &pb.RequestNodeIPsRequest{
		ChunkNumber: int32(len(chunks)),
		FileName:    "Lab2Vid.mp4",
	}

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

func sendChuksToServer() {

}
