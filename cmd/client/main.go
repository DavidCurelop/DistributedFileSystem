package main

import (
	//"bufio"
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"distributedFileSystem/pkg/config"
	"distributedFileSystem/pkg/failover"
	pb "distributedFileSystem/pkg/proto/client"
	cnpb "distributedFileSystem/pkg/proto/controlnode"
	datanodePb "distributedFileSystem/pkg/proto/datanode"
	"distributedFileSystem/pkg/security"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const defaultPartitionSize = 3 * 1024 * 1024

// dialOptions carries the TLS credentials and auth token used by every outgoing connection.
// It is set once in main before any command runs.
var dialOptions []grpc.DialOption

type clientApp struct {
	controlNodeConn *grpc.ClientConn
}

func main() {
	ctx := context.Background()
	controlNodeAddress := flag.String("ctrlAdd", config.EnvString("CONTROL_ADDR", "localhost:50051"), "Comma-separated ControlNode addresses, host[:port] (env CONTROL_ADDR)")
	authToken := flag.String("auth-token", config.EnvString("AUTH_TOKEN", security.DefaultAuthToken), "Token sent to the ControlNode and DataNodes (env AUTH_TOKEN)")
	tlsCA := flag.String("tls-ca", config.EnvString("TLS_CA_FILE", ""), "CA PEM file used to verify server certificates; empty skips verification (env TLS_CA_FILE)")
	flag.Parse()

	if err := security.ValidateToken(*authToken); err != nil {
		log.Fatalf("invalid auth token: %v", err)
	}
	var err error
	dialOptions, err = security.ClientDialOptions(*tlsCA, *authToken)
	if err != nil {
		log.Fatalf("configuring TLS: %v", err)
	}

	controlNodeAddresses := config.AddressList(*controlNodeAddress, "50051")
	if len(controlNodeAddresses) == 0 {
		log.Fatalf("at least one ControlNode address is required")
	}

	controlNodeConn, err := failover.NewControlNodeClient(controlNodeAddresses, dialOptions...)
	if err != nil {
		log.Fatalf("connecting to ControlNodes %v: %v", controlNodeAddresses, err)
	}
	defer controlNodeConn.Close()

	app := &clientApp{controlNodeConn: controlNodeConn}
	if len(flag.Args()) > 0 {
		err := app.dispatchCommand(ctx, flag.Args())
		if err != nil {
			log.Fatalf("dispatching command %v", err)
		}
		return
	}
	runInteractiveShell(ctx, app)
}

func (app *clientApp) dispatchCommand(ctx context.Context, commandTokens []string) error {
	// Guard clause: empty input
	if len(commandTokens) == 0 {
		return nil
	}
	commandName := strings.ToLower(commandTokens[0])
	commandArgs := commandTokens[1:]
	switch commandName {
	case "put":
		return app.handlePut(ctx, commandArgs)
	case "get":
		return app.handleGet(ctx, commandArgs)
	case "ls":
		return app.handleList(ctx, commandArgs)
	case "mkdir":
		return app.handleMkdir(ctx, commandArgs)
	case "rmdir":
		return app.handleRmdir(ctx, commandArgs)
	case "rm":
		return app.handleRemove(ctx, commandArgs)
	case "exit", "quit":
		os.Exit(0)
		return nil
	default:
		return fmt.Errorf("unknown command: %q (supported: put, get, ls, mkdir, rmdir, rm, exit)", commandName)
	}
}
func runInteractiveShell(ctx context.Context, app *clientApp) {
	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("dfsha> ")
		input := scanner.Scan()
		if !input {
			break
		}
		text := strings.Fields(scanner.Text())
		terminalctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := app.dispatchCommand(terminalctx, text)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		}
		cancel()
	}
}
func (app *clientApp) handlePut(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: put <file_path>")
	}
	localSourcePath := args[0]
	//fileName := filepath.Base(localSourcePath)
	dfsPath := "/"
	if len(args) >= 2 {
		dfsPath = path.Join(args[1], filepath.Base(localSourcePath))
	}

	if len(args) == 1 {
		dfsPath = path.Join(dfsPath, filepath.Base(localSourcePath))
	}

	// Measure size of local source file
	fileInfo, err := os.Stat(localSourcePath)
	if err != nil {
		return fmt.Errorf("stating source file %q: %w", localSourcePath, err)
	}
	fileSize := fileInfo.Size()

	chunks, err := partition(localSourcePath)
	if err != nil {
		return fmt.Errorf("partitioning file %q: %w", localSourcePath, err)
	}

	client := cnpb.NewFileMetadataServiceClient(app.controlNodeConn)
	createFileResponse, err := client.CreateFileEntry(ctx, &cnpb.CreateFileEntryRequest{
		Path:        dfsPath,
		Size:        fileSize,
		TotalChunks: int32(len(chunks)),
		ChunkSize:   int32(defaultPartitionSize),
	})

	if err != nil {
		return fmt.Errorf("creating file entry %q: %w", dfsPath, err)
	}

	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	flattenedFileName := encodeFlatChunkPrefix(dfsPath)
	if err := sendChunksToServer(ctx, flattenedFileName, chunks, createFileResponse.Chunks); err != nil {
		return fmt.Errorf("failed to upload partitions: %v", err)
	}
	log.Printf("All %d partitions successfully transmitted and stored!\n", len(chunks))
	return nil
}
func (app *clientApp) handleGet(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: get <remote_path> [local_path]")
	}
	remotePath := args[0]

	localPath := filepath.Base(remotePath)
	if len(args) >= 2 {
		localPath = args[1]
	}
	// Like cp: downloading into an existing directory keeps the remote file name.
	if info, err := os.Stat(localPath); err == nil && info.IsDir() {
		localPath = filepath.Join(localPath, filepath.Base(remotePath))
	}

	if err := downloadFile(ctx, app.controlNodeConn, remotePath, localPath); err != nil {
		return fmt.Errorf("downloading %q: %w", remotePath, err)
	}
	fmt.Printf("downloaded %s to %s\n", remotePath, localPath)
	return nil
}
func (app *clientApp) handleList(ctx context.Context, args []string) error {
	tagetDirPath := "/"
	if len(args) > 0 {
		tagetDirPath = args[0]
	}
	client := cnpb.NewFileMetadataServiceClient(app.controlNodeConn)
	listDirectoryResponse, err := client.ListDirectory(ctx, &cnpb.ListDirectoryRequest{Path: tagetDirPath})
	if err != nil {
		return fmt.Errorf("listing directory %v: %w", tagetDirPath, err)
	}

	entries := listDirectoryResponse.Entries

	if len(entries) == 0 {
		fmt.Fprintf(os.Stdout, "directory '%v' is empty\n", tagetDirPath)
	}

	for _, entry := range entries {
		isDir := entry.IsDirectory
		name := entry.Name
		size := entry.Size
		if isDir {
			fmt.Printf("[Dir] %v, size: %v\n", name, size)
		} else {
			fmt.Printf("[File] %v, size: %v\n", name, size)
		}
	}

	return nil
}
func (app *clientApp) handleRemove(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: rm <file_path>")
	}
	filePath := args[0]

	client := cnpb.NewFileMetadataServiceClient(app.controlNodeConn)
	deleteFileResponse, err := client.DeleteFile(ctx, &cnpb.DeleteFileRequest{Path: filePath})

	if err != nil {
		return fmt.Errorf("deleting file %v: %w", filePath, err)
	}

	if !deleteFileResponse.Deleted {
		return fmt.Errorf("file %v was not deleted", filePath)

	}

	fmt.Printf("successfully deleted %v\n", filePath)
	return nil
}

func (app *clientApp) handleMkdir(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: mkdir <directory_path>")
	}
	dirPath := args[0]

	client := cnpb.NewFileMetadataServiceClient(app.controlNodeConn)

	createDirectoryResponse, err := client.CreateDirectory(ctx, &cnpb.CreateDirectoryRequest{Path: dirPath})

	if err != nil {
		return fmt.Errorf("creating directory %v: %w", dirPath, err)
	}

	if !createDirectoryResponse.Created {
		return fmt.Errorf("directory %v was not created", dirPath)

	}

	fmt.Printf("created directory %s\n", dirPath)
	return nil
}

func (app *clientApp) handleRmdir(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: rmdir <directory_path>")
	}

	dirPath := args[0]

	client := cnpb.NewFileMetadataServiceClient(app.controlNodeConn)

	deleteDirectoryResponse, err := client.DeleteDirectory(ctx, &cnpb.DeleteDirectoryRequest{Path: dirPath})

	if err != nil {
		return fmt.Errorf("deleting directory %v: %w", dirPath, err)
	}

	if !deleteDirectoryResponse.Deleted {
		return fmt.Errorf("directory %v was not deleted", dirPath)

	}

	fmt.Printf("successfully deleted %v\n", dirPath)
	return nil
}

func getPartitionAddresses(controlNodeConn *grpc.ClientConn, requestPayload *pb.RequestNodeIPsRequest) (*pb.RequestNodeIPsResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	fileSavingClient := pb.NewFileSavingClient(controlNodeConn)

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

	dataNodeConnection, err := grpc.NewClient(dataNodeIP, dialOptions...)
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

// Neutral example: Encoding a virtual hierarchy into a flat, safe storage key
func encodeFlatChunkPrefix(virtualHierarchyPath string) string {
	// 1. Remove leading slash so we don't start with an underscore
	trimmed := strings.TrimPrefix(virtualHierarchyPath, "/")
	// 2. Replace internal slashes with a safe separator
	return strings.ReplaceAll(trimmed, "/", "_")
}

func sendChunksToServer(ctx context.Context, fileName string, chunks [][]byte, allocations []*cnpb.ChunkAllocation) error {
	chunkLen := len(chunks)

	for chunkIndex, payload := range chunks {
		chunkID := allocations[chunkIndex].ChunkId
		if len(allocations[chunkIndex].NodeAddresses) == 0 {
			return fmt.Errorf("chunk len must be at least 1")
		}

		primaryNode := allocations[chunkIndex].NodeAddresses[0]
		replicaTargets := allocations[chunkIndex].NodeAddresses[1:]
		_, err := sendChunk(ctx, primaryNode, replicaTargets, chunkID, payload, 3)

		if err != nil {
			return fmt.Errorf("dispatching chunk %s to primary %s: %w", chunkID, primaryNode, err)
		}
		percent := (chunkIndex + 1) * 100 / chunkLen
		fmt.Printf("\rUploading %s: %d/%d chunks (%d%%)", fileName, chunkIndex+1, chunkLen, percent)
	}
	fmt.Println()
	return nil
}
