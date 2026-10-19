package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const forwardedHeader = "x-dfsha-forwarded"

type leaderForwarder struct {
	dialOptions          []grpc.DialOption
	leaderConnectionLock sync.Mutex
	leaderConnections    map[raft.ServerAddress]*grpc.ClientConn
}

func newLeaderForwarder(dialOptions []grpc.DialOption) *leaderForwarder {
	return &leaderForwarder{
		dialOptions:       dialOptions,
		leaderConnections: make(map[raft.ServerAddress]*grpc.ClientConn),
	}
}

func (f *leaderForwarder) connection(leaderAddress raft.ServerAddress) (*grpc.ClientConn, error) {
	f.leaderConnectionLock.Lock()
	defer f.leaderConnectionLock.Unlock()

	if leaderConnection, cached := f.leaderConnections[leaderAddress]; cached {
		return leaderConnection, nil
	}
	leaderConnection, err := grpc.NewClient(string(leaderAddress), f.dialOptions...)
	if err != nil {
		return nil, fmt.Errorf("connecting to leader ControlNode %q: %w", leaderAddress, err)
	}
	f.leaderConnections[leaderAddress] = leaderConnection
	return leaderConnection, nil
}

func (s *controlNodeServer) leaderConnection(ctx context.Context) (*grpc.ClientConn, context.Context, error) {
	if isForwardedRequest(ctx) {
		return nil, nil, status.Error(codes.Unavailable, "this ControlNode is no longer the leader; retry the request")
	}
	leaderAddress, err := s.consensus.waitForLeader(ctx)
	if err != nil {
		return nil, nil, err
	}
	leaderConnection, err := s.forwarder.connection(leaderAddress)
	if err != nil {
		return nil, nil, status.Error(codes.Unavailable, err.Error())
	}
	forwardedCtx := metadata.AppendToOutgoingContext(ctx, forwardedHeader, "true")
	return leaderConnection, forwardedCtx, nil
}

func isForwardedRequest(ctx context.Context) bool {
	incomingMetadata, found := metadata.FromIncomingContext(ctx)
	return found && len(incomingMetadata.Get(forwardedHeader)) > 0
}
