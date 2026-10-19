package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	cnpb "distributedFileSystem/pkg/proto/controlnode"
	datanodePb "distributedFileSystem/pkg/proto/datanode"

	"google.golang.org/grpc"
)

const (
	metadataTimeout     = 5 * time.Second
	chunkFetchTimeout   = 10 * time.Second
	downloadFilePerm    = 0o644
	partialFileTemplate = ".%s.part-*"
)

// downloadFile retrieves remotePath from the cluster and writes it to outputPath.
// The chunks are fetched in order and assembled into a temporary file that
// replaces outputPath only after every chunk has been verified.
func downloadFile(ctx context.Context, controlNodeConn *grpc.ClientConn, remotePath, outputPath string) error {
	metadata, err := getFileMetadata(ctx, controlNodeConn, remotePath)
	if err != nil {
		return err
	}
	chunks, err := orderedChunks(metadata)
	if err != nil {
		return fmt.Errorf("invalid metadata for %q: %w", remotePath, err)
	}

	partialFile, err := os.CreateTemp(filepath.Dir(outputPath), fmt.Sprintf(partialFileTemplate, filepath.Base(outputPath)))
	if err != nil {
		return fmt.Errorf("creating temporary file for %q: %w", outputPath, err)
	}
	partialPath := partialFile.Name()
	completed := false
	defer func() {
		if !completed {
			partialFile.Close()
			os.Remove(partialPath)
		}
	}()

	var totalWritten int64
	for _, chunk := range chunks {
		payload, err := fetchChunk(ctx, chunk)
		if err != nil {
			return fmt.Errorf("downloading chunk %d of %q: %w", chunk.ChunkIndex, remotePath, err)
		}
		if _, err := partialFile.Write(payload); err != nil {
			return fmt.Errorf("writing chunk %d to %q: %w", chunk.ChunkIndex, partialPath, err)
		}
		totalWritten += int64(len(payload))
		log.Printf("Chunk %d/%d downloaded (%d bytes)", chunk.ChunkIndex+1, len(chunks), len(payload))
	}

	if metadata.Size > 0 && totalWritten != metadata.Size {
		return fmt.Errorf("assembled %d bytes but %q is %d bytes long", totalWritten, remotePath, metadata.Size)
	}
	if err := partialFile.Sync(); err != nil {
		return fmt.Errorf("syncing %q: %w", partialPath, err)
	}
	if err := partialFile.Close(); err != nil {
		return fmt.Errorf("closing %q: %w", partialPath, err)
	}
	if err := os.Chmod(partialPath, downloadFilePerm); err != nil {
		return fmt.Errorf("setting permissions on %q: %w", partialPath, err)
	}
	if err := os.Rename(partialPath, outputPath); err != nil {
		return fmt.Errorf("moving %q to %q: %w", partialPath, outputPath, err)
	}
	completed = true

	log.Printf("Downloaded %q to %q (%d bytes, %d chunks)", remotePath, outputPath, totalWritten, len(chunks))
	return nil
}

func getFileMetadata(ctx context.Context, controlNodeConn *grpc.ClientConn, remotePath string) (*cnpb.FileMetadata, error) {
	callCtx, cancel := context.WithTimeout(ctx, metadataTimeout)
	defer cancel()

	response, err := cnpb.NewFileMetadataServiceClient(controlNodeConn).GetFileMetadata(callCtx, &cnpb.GetFileMetadataRequest{Path: remotePath})
	if err != nil {
		return nil, fmt.Errorf("getting metadata of %q: %w", remotePath, err)
	}
	if response.GetMetadata() == nil {
		return nil, fmt.Errorf("getting metadata of %q: empty response", remotePath)
	}
	return response.Metadata, nil
}

// orderedChunks returns the chunks sorted by index and checks that they form a
// contiguous 0..n-1 sequence matching the declared chunk count.
func orderedChunks(metadata *cnpb.FileMetadata) ([]*cnpb.ChunkAllocation, error) {
	chunks := append([]*cnpb.ChunkAllocation(nil), metadata.Chunks...)
	sort.Slice(chunks, func(i, j int) bool { return chunks[i].ChunkIndex < chunks[j].ChunkIndex })

	if int(metadata.TotalChunks) != len(chunks) {
		return nil, fmt.Errorf("expected %d chunks but metadata lists %d", metadata.TotalChunks, len(chunks))
	}
	for i, chunk := range chunks {
		if int(chunk.ChunkIndex) != i {
			return nil, fmt.Errorf("chunk index %d is missing or duplicated", i)
		}
	}
	return chunks, nil
}

// fetchChunk tries each replica of the chunk in turn and returns the first
// payload that passes verification.
func fetchChunk(ctx context.Context, chunk *cnpb.ChunkAllocation) ([]byte, error) {
	if len(chunk.NodeAddresses) == 0 {
		return nil, fmt.Errorf("chunk %q has no replicas", chunk.ChunkId)
	}

	var failures []error
	for _, address := range chunk.NodeAddresses {
		payload, err := fetchChunkFromNode(ctx, address, chunk)
		if err == nil {
			return payload, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		log.Printf("Replica %s failed for chunk %q: %v", address, chunk.ChunkId, err)
		failures = append(failures, fmt.Errorf("replica %s: %w", address, err))
	}
	return nil, fmt.Errorf("all %d replicas failed: %w", len(chunk.NodeAddresses), errors.Join(failures...))
}

func fetchChunkFromNode(ctx context.Context, address string, chunk *cnpb.ChunkAllocation) ([]byte, error) {
	conn, err := grpc.NewClient(address, dialOptions...)
	if err != nil {
		return nil, fmt.Errorf("connecting: %w", err)
	}
	defer conn.Close()

	callCtx, cancel := context.WithTimeout(ctx, chunkFetchTimeout)
	defer cancel()

	response, err := datanodePb.NewDataNodeServiceClient(conn).GetChunk(callCtx, &datanodePb.GetChunkRequest{ChunkId: chunk.ChunkId})
	if err != nil {
		return nil, fmt.Errorf("getting chunk: %w", err)
	}
	if err := verifyChunk(chunk, response); err != nil {
		return nil, err
	}
	return response.ChunkPayload, nil
}

// verifyChunk recomputes the SHA-256 of the received payload and compares it
// with the checksum recorded in the metadata. When the metadata carries no
// checksum, the one reported by the DataNode is used instead.
func verifyChunk(chunk *cnpb.ChunkAllocation, response *datanodePb.GetChunkResponse) error {
	expected := chunk.CheckSum
	if expected == "" {
		expected = response.CheckSum
	}
	if expected == "" {
		return errors.New("no checksum available to verify the chunk")
	}

	sum := sha256.Sum256(response.ChunkPayload)
	if actual := hex.EncodeToString(sum[:]); !strings.EqualFold(actual, expected) {
		return fmt.Errorf("checksum mismatch: expected %s, got %s", expected, actual)
	}
	if chunk.ChunkSize > 0 && int64(len(response.ChunkPayload)) != chunk.ChunkSize {
		return fmt.Errorf("size mismatch: expected %d bytes, got %d", chunk.ChunkSize, len(response.ChunkPayload))
	}
	return nil
}
