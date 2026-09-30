package main

import (
	"fmt"
	"io"
	"log"
	"os"
)

const defaultPartitionSize = 4 * 1024 

func main() {
	chunks, err := partition("C:/Users/david/Documents/DistributedSystems/Proyect1/SI3007-262-proyecto1-dfs-beta.pdf")
	if err != nil {
		log.Fatalf("Partition failed: %v", err)
	}
	log.Printf("Successfully created %d partition(s)!", len(chunks))
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
