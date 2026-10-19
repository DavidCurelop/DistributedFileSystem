// Package failover dials a set of interchangeable ControlNodes as a single
// gRPC channel, so callers keep working when one ControlNode goes down.
package failover

import (
	"errors"
	"fmt"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
)

// resolverScheme names the static resolver that hands the ControlNode list to
// the gRPC channel.
const resolverScheme = "dfsha-controlnodes"

// controlNodeServiceConfig makes the channel stick to the first reachable
// ControlNode (pick_first walks the list in order when the current one fails)
// and transparently retries calls that fail with UNAVAILABLE. That is the code
// a caller sees while a ControlNode is down or a new Raft leader is being
// elected. gRPC caps maxAttempts at 5.
const controlNodeServiceConfig = `{
  "loadBalancingConfig": [{"pick_first": {}}],
  "methodConfig": [{
    "name": [{}],
    "retryPolicy": {
      "maxAttempts": 5,
      "initialBackoff": "0.5s",
      "maxBackoff": "4s",
      "backoffMultiplier": 2,
      "retryableStatusCodes": ["UNAVAILABLE"]
    }
  }]
}`

// NewControlNodeClient returns one channel spanning every ControlNode address.
// Any ControlNode can serve any call because followers forward to the Raft
// leader, so the channel only needs to reach one live node.
func NewControlNodeClient(controlNodeAddresses []string, dialOptions ...grpc.DialOption) (*grpc.ClientConn, error) {
	if len(controlNodeAddresses) == 0 {
		return nil, errors.New("at least one ControlNode address is required")
	}

	controlNodeEndpoints := make([]resolver.Endpoint, 0, len(controlNodeAddresses))
	for _, controlNodeAddress := range controlNodeAddresses {
		controlNodeHost, _, err := net.SplitHostPort(controlNodeAddress)
		if err != nil {
			return nil, fmt.Errorf("parsing ControlNode address %q: %w", controlNodeAddress, err)
		}
		// ServerName keeps TLS verification per ControlNode; otherwise the
		// authority would be the synthetic target name below.
		controlNodeEndpoints = append(controlNodeEndpoints, resolver.Endpoint{
			Addresses: []resolver.Address{{Addr: controlNodeAddress, ServerName: controlNodeHost}},
		})
	}

	controlNodeResolver := manual.NewBuilderWithScheme(resolverScheme)
	controlNodeResolver.InitialState(resolver.State{Endpoints: controlNodeEndpoints})

	channelOptions := append([]grpc.DialOption{
		grpc.WithResolvers(controlNodeResolver),
		grpc.WithDefaultServiceConfig(controlNodeServiceConfig),
	}, dialOptions...)

	controlNodeConnection, err := grpc.NewClient(resolverScheme+":///controlnodes", channelOptions...)
	if err != nil {
		return nil, fmt.Errorf("creating channel to ControlNodes %v: %w", controlNodeAddresses, err)
	}
	return controlNodeConnection, nil
}
