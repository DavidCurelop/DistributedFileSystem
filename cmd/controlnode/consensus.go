package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"distributedFileSystem/pkg/config"

	transport "github.com/Jille/raft-grpc-transport"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	raftApplyTimeout = 10 * time.Second
	leaderWaitTimeout  = 5 * time.Second
	leaderPollInterval = 100 * time.Millisecond
	raftSnapshotRetain = 2
	raftLogStoreFile   = "raft.db"
	defaultRaftPort    = "50051"
)

type raftPeer struct {
	id      raft.ServerID
	address raft.ServerAddress
}

func parseRaftPeers(rawPeers string) ([]raftPeer, error) {
	var peers []raftPeer
	seenPeerIDs := make(map[string]bool)

	for _, rawPeer := range strings.Split(rawPeers, ",") {
		rawPeer = strings.TrimSpace(rawPeer)
		if rawPeer == "" {
			continue
		}
		peerID, peerAddress, found := strings.Cut(rawPeer, "=")
		if !found || peerID == "" || peerAddress == "" {
			return nil, fmt.Errorf("raft peer %q must have the form id=host:port", rawPeer)
		}
		if seenPeerIDs[peerID] {
			return nil, fmt.Errorf("raft peer id %q is listed more than once", peerID)
		}
		seenPeerIDs[peerID] = true
		peers = append(peers, raftPeer{
			id:      raft.ServerID(peerID),
			address: raft.ServerAddress(config.WithDefaultPort(peerAddress, defaultRaftPort)),
		})
	}

	if len(peers) == 0 {
		return nil, errors.New("at least one raft peer is required")
	}
	return peers, nil
}

func findLocalPeer(peers []raftPeer, localNodeID string) (raftPeer, error) {
	for _, peer := range peers {
		if string(peer.id) == localNodeID {
			return peer, nil
		}
	}
	return raftPeer{}, fmt.Errorf("node id %q is not listed in the raft peers", localNodeID)
}

type consensusSettings struct {
	localPeer     raftPeer
	peers         []raftPeer
	dataDirectory string
	dialOptions   []grpc.DialOption
}

type consensusNode struct {
	raftNode         *raft.Raft
	transportManager *transport.Manager
	localAddress     raft.ServerAddress
	logStore         *raftboltdb.BoltStore
}

func newConsensusNode(settings consensusSettings, fsm raft.FSM) (*consensusNode, error) {
	if err := os.MkdirAll(settings.dataDirectory, 0o755); err != nil {
		return nil, fmt.Errorf("creating raft directory %q: %w", settings.dataDirectory, err)
	}

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = settings.localPeer.id
	raftConfig.Logger = hclog.New(&hclog.LoggerOptions{
		Name:  "raft-" + string(settings.localPeer.id),
		Level: hclog.Info,
	})

	logStorePath := filepath.Join(settings.dataDirectory, raftLogStoreFile)
	logStore, err := raftboltdb.NewBoltStore(logStorePath)
	if err != nil {
		return nil, fmt.Errorf("opening raft log store %q: %w", logStorePath, err)
	}
	snapshotStore, err := raft.NewFileSnapshotStore(settings.dataDirectory, raftSnapshotRetain, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("opening raft snapshot store %q: %w", settings.dataDirectory, err)
	}
	hasExistingState, err := raft.HasExistingState(logStore, logStore, snapshotStore)
	if err != nil {
		return nil, fmt.Errorf("inspecting raft state in %q: %w", settings.dataDirectory, err)
	}

	transportManager := transport.New(settings.localPeer.address, settings.dialOptions)
	raftNode, err := raft.NewRaft(raftConfig, fsm, logStore, logStore, snapshotStore, transportManager.Transport())
	if err != nil {
		return nil, fmt.Errorf("starting raft node %q: %w", settings.localPeer.id, err)
	}

	if !hasExistingState {
		bootstrapConfiguration := raft.Configuration{Servers: make([]raft.Server, 0, len(settings.peers))}
		for _, peer := range settings.peers {
			bootstrapConfiguration.Servers = append(bootstrapConfiguration.Servers, raft.Server{
				Suffrage: raft.Voter,
				ID:       peer.id,
				Address:  peer.address,
			})
		}
		err := raftNode.BootstrapCluster(bootstrapConfiguration).Error()
		if err != nil && !errors.Is(err, raft.ErrCantBootstrap) {
			return nil, fmt.Errorf("bootstrapping raft cluster with %d peers: %w", len(settings.peers), err)
		}
	}

	return &consensusNode{
		raftNode:         raftNode,
		transportManager: transportManager,
		localAddress:     settings.localPeer.address,
		logStore:         logStore,
	}, nil
}

func (c *consensusNode) Close() error {
	var errs []error
	if c.raftNode != nil {
		if err := c.raftNode.Shutdown().Error(); err != nil {
			errs = append(errs, err)
		}
	}
	if c.transportManager != nil {
		if err := c.transportManager.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if c.logStore != nil {
		if err := c.logStore.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (c *consensusNode) registerTransport(grpcServer grpc.ServiceRegistrar) {
	c.transportManager.Register(grpcServer)
}

func (c *consensusNode) isLeader() bool {
	return c.raftNode.State() == raft.Leader
}

func (c *consensusNode) apply(ctx context.Context, command fsmCommand) error {
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	encodedCommand, err := json.Marshal(command)
	if err != nil {
		return status.Errorf(codes.Internal, "encoding %s command for %q: %v", command.Type, command.Path, err)
	}

	applyFuture := c.raftNode.Apply(encodedCommand, enqueueTimeout(ctx))
	if err := awaitFuture(ctx, applyFuture); err != nil {
		return translateRaftError(fmt.Sprintf("replicating %s of %q", command.Type, command.Path), err)
	}
	if applyErr, isError := applyFuture.Response().(error); isError {
		return applyErr
	}
	return nil
}

func (c *consensusNode) verifyLeader(ctx context.Context) error {
	if err := awaitFuture(ctx, c.raftNode.VerifyLeader()); err != nil {
		return translateRaftError("verifying leadership before read", err)
	}
	return nil
}

func (c *consensusNode) waitForLeader(ctx context.Context) (raft.ServerAddress, error) {
	waitCtx, cancelWait := context.WithTimeout(ctx, leaderWaitTimeout)
	defer cancelWait()
	leaderPollTicker := time.NewTicker(leaderPollInterval)
	defer leaderPollTicker.Stop()

	for {
		leaderAddress, _ := c.raftNode.LeaderWithID()
		if leaderAddress != "" {
			return leaderAddress, nil
		}
		select {
		case <-waitCtx.Done():
			if err := ctx.Err(); err != nil {
				return "", status.FromContextError(err).Err()
			}
			return "", status.Error(codes.Unavailable, "no ControlNode leader is elected yet; retry shortly")
		case <-leaderPollTicker.C:
		}
	}
}

func awaitFuture(ctx context.Context, future raft.Future) error {
	futureResult := make(chan error, 1)
	go func() { futureResult <- future.Error() }()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-futureResult:
		return err
	}
}

func enqueueTimeout(ctx context.Context) time.Duration {
	deadline, hasDeadline := ctx.Deadline()
	if !hasDeadline {
		return raftApplyTimeout
	}
	return min(raftApplyTimeout, time.Until(deadline))
}

func translateRaftError(action string, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	switch {
	case errors.Is(err, raft.ErrNotLeader),
		errors.Is(err, raft.ErrLeadershipLost),
		errors.Is(err, raft.ErrLeadershipTransferInProgress),
		errors.Is(err, raft.ErrEnqueueTimeout),
		errors.Is(err, raft.ErrRaftShutdown):
		return status.Errorf(codes.Unavailable, "%s: %v; retry against the new leader", action, err)
	default:
		return status.Errorf(codes.Internal, "%s: %v", action, err)
	}
}
