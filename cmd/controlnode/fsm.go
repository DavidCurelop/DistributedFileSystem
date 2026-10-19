package main

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"

	cnpb "distributedFileSystem/pkg/proto/controlnode"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type fsmCommandType string

const (
	commandCreateFile      fsmCommandType = "create_file"
	commandCreateDirectory fsmCommandType = "create_directory"
	commandDeleteFile      fsmCommandType = "delete_file"
	commandDeleteDirectory fsmCommandType = "delete_directory"
	commandAddReplica      fsmCommandType = "add_replica"
)

type fsmCommand struct {
	Type          fsmCommandType `json:"type"`
	Path          string         `json:"path"`
	Owner         string         `json:"owner,omitempty"`
	CreatedAtUnix int64          `json:"created_at_unix,omitempty"`
	FileMetadata  []byte         `json:"file_metadata,omitempty"` 
	ChunkID       string         `json:"chunk_id,omitempty"`
	NodeAddress   string         `json:"node_address,omitempty"`
}

func newCreateFileCommand(metadata *cnpb.FileMetadata, owner string) (fsmCommand, error) {
	encodedMetadata, err := proto.Marshal(metadata)
	if err != nil {
		return fsmCommand{}, status.Errorf(codes.Internal, "encoding metadata of %q: %v", metadata.Path, err)
	}
	return fsmCommand{Type: commandCreateFile, Path: metadata.Path, Owner: owner, FileMetadata: encodedMetadata}, nil
}

type namespaceFSM struct {
	files *fileRegistry
}

var _ raft.FSM = (*namespaceFSM)(nil)

func (fsm *namespaceFSM) Apply(logEntry *raft.Log) any {
	var command fsmCommand
	if err := json.Unmarshal(logEntry.Data, &command); err != nil {
		return status.Errorf(codes.Internal, "decoding raft log entry %d: %v", logEntry.Index, err)
	}

	switch command.Type {
	case commandCreateFile:
		metadata := &cnpb.FileMetadata{}
		if err := proto.Unmarshal(command.FileMetadata, metadata); err != nil {
			return status.Errorf(codes.Internal, "decoding metadata of %q in raft log entry %d: %v", command.Path, logEntry.Index, err)
		}
		return fsm.files.createFile(metadata, command.Owner)
	case commandCreateDirectory:
		return fsm.files.mkdir(command.Path, command.CreatedAtUnix, command.Owner)
	case commandDeleteFile:
		return fsm.files.deleteFile(command.Path)
	case commandDeleteDirectory:
		return fsm.files.rmdir(command.Path)
	case commandAddReplica:
		fsm.files.addReplica(command.Path, command.ChunkID, command.NodeAddress)
		return nil
	default:
		return status.Errorf(codes.Internal, "unknown command type %q in raft log entry %d", command.Type, logEntry.Index)
	}
}

func (fsm *namespaceFSM) Snapshot() (raft.FSMSnapshot, error) {
	snapshot, err := fsm.files.exportSnapshot()
	if err != nil {
		return nil, fmt.Errorf("capturing namespace snapshot: %w", err)
	}
	return snapshot, nil
}

func (fsm *namespaceFSM) Restore(snapshotReader io.ReadCloser) error {
	defer snapshotReader.Close()

	var snapshot namespaceSnapshot
	if err := json.NewDecoder(snapshotReader).Decode(&snapshot); err != nil {
		return fmt.Errorf("decoding namespace snapshot: %w", err)
	}
	if err := fsm.files.importSnapshot(&snapshot); err != nil {
		return fmt.Errorf("restoring namespace snapshot: %w", err)
	}
	return nil
}

type namespaceSnapshotEntry struct {
	Path          string `json:"path"`
	IsDirectory   bool   `json:"is_directory"`
	Owner         string `json:"owner,omitempty"`
	CreatedAtUnix int64  `json:"created_at_unix,omitempty"`
	FileMetadata  []byte `json:"file_metadata,omitempty"` 
}

type namespaceSnapshot struct {
	RootCreatedAtUnix int64                    `json:"root_created_at_unix"`
	Entries           []namespaceSnapshotEntry `json:"entries"`
}

var _ raft.FSMSnapshot = (*namespaceSnapshot)(nil)

func (snapshot *namespaceSnapshot) Persist(sink raft.SnapshotSink) error {
	if err := json.NewEncoder(sink).Encode(snapshot); err != nil {
		sink.Cancel()
		return fmt.Errorf("writing namespace snapshot %s: %w", sink.ID(), err)
	}
	return sink.Close()
}

func (snapshot *namespaceSnapshot) Release() {}

func (r *fileRegistry) exportSnapshot() (*namespaceSnapshot, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	snapshot := &namespaceSnapshot{RootCreatedAtUnix: r.root.createdAt}
	if err := appendSnapshotEntries(&snapshot.Entries, r.root, nil); err != nil {
		return nil, err
	}
	return snapshot, nil
}

func appendSnapshotEntries(entries *[]namespaceSnapshotEntry, dir *namespaceNode, dirParts []string) error {
	names := make([]string, 0, len(dir.children))
	for name := range dir.children {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		child := dir.children[name]
		childParts := append(append([]string(nil), dirParts...), name)
		childPath := joinPath(childParts)

		if !child.isDir {
			encodedMetadata, err := proto.Marshal(child.metadata)
			if err != nil {
				return fmt.Errorf("encoding metadata of %q: %w", childPath, err)
			}
			*entries = append(*entries, namespaceSnapshotEntry{Path: childPath, Owner: child.owner, FileMetadata: encodedMetadata})
			continue
		}

		*entries = append(*entries, namespaceSnapshotEntry{Path: childPath, IsDirectory: true, Owner: child.owner, CreatedAtUnix: child.createdAt})
		if err := appendSnapshotEntries(entries, child, childParts); err != nil {
			return err
		}
	}
	return nil
}

func (r *fileRegistry) importSnapshot(snapshot *namespaceSnapshot) error {
	restored := &fileRegistry{root: newDirNode(snapshot.RootCreatedAtUnix, "")}
	for _, entry := range snapshot.Entries {
		restoredNode, err := entry.namespaceNode()
		if err != nil {
			return err
		}
		if err := restored.insert(entry.Path, restoredNode); err != nil {
			return fmt.Errorf("inserting %q: %w", entry.Path, err)
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.root = restored.root
	return nil
}

func (entry namespaceSnapshotEntry) namespaceNode() (*namespaceNode, error) {
	if entry.IsDirectory {
		return newDirNode(entry.CreatedAtUnix, entry.Owner), nil
	}
	metadata := &cnpb.FileMetadata{}
	if err := proto.Unmarshal(entry.FileMetadata, metadata); err != nil {
		return nil, fmt.Errorf("decoding metadata of %q: %w", entry.Path, err)
	}
	return &namespaceNode{metadata: metadata, owner: entry.Owner}, nil
}
