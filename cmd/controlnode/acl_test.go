package main

import (
	"testing"
	"time"

	cnpb "distributedFileSystem/pkg/proto/controlnode"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestFileRegistryAccessControl(t *testing.T) {
	files := newFileRegistry()

	if err := files.mkdir("/alice", time.Now().Unix(), "alice"); err != nil {
		t.Fatalf("failed to create /alice: %v", err)
	}

	metadata := &cnpb.FileMetadata{
		Path:          "/alice/file.txt",
		Size:          100,
		TotalChunks:   1,
		CreatedAtUnix: time.Now().Unix(),
	}
	if err := files.createFile(metadata, "alice"); err != nil {
		t.Fatalf("failed to create /alice/file.txt: %v", err)
	}

	entries, err := files.list("/alice", "alice")
	if err != nil {
		t.Fatalf("alice failed to list /alice: %v", err)
	}
	if len(entries) != 1 || entries[0].Name != "file.txt" {
		t.Fatalf("expected file.txt in entries, got %v", entries)
	}

	gotMeta, err := files.get("/alice/file.txt", "alice")
	if err != nil {
		t.Fatalf("alice failed to get /alice/file.txt: %v", err)
	}
	if gotMeta.Path != "/alice/file.txt" {
		t.Fatalf("unexpected path: %v", gotMeta.Path)
	}

	_, err = files.list("/alice", "bob")
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied for bob listing /alice, got: %v", err)
	}

	_, err = files.get("/alice/file.txt", "bob")
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied for bob getting /alice/file.txt, got: %v", err)
	}

	if err := files.checkParentAccess("/alice/intruder.txt", "bob"); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied for bob writing to /alice, got: %v", err)
	}

	if err := files.checkNodeAccess("/alice/file.txt", "bob"); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied for bob deleting /alice/file.txt, got: %v", err)
	}

	entriesAdmin, err := files.list("/alice", "admin")
	if err != nil {
		t.Fatalf("admin failed to list /alice: %v", err)
	}
	if len(entriesAdmin) != 1 {
		t.Fatalf("expected admin to see 1 entry, got %d", len(entriesAdmin))
	}

	if err := files.checkNodeAccess("/alice/file.txt", "admin"); err != nil {
		t.Fatalf("admin should have access to /alice/file.txt: %v", err)
	}

	rootEntries, err := files.list("/", "bob")
	if err != nil {
		t.Fatalf("bob failed to list root: %v", err)
	}
	if len(rootEntries) != 1 || rootEntries[0].Name != "alice" {
		t.Fatalf("expected alice directory in root entries, got %v", rootEntries)
	}
}
