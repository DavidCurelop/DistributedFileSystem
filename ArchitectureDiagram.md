:::mermaid
graph TD
    subgraph Client Node
        CLI[Client CLI / API]
    end

    subgraph Control Plane
        CN1[ControlNode Leader]
        CN2[ControlNode Follower]
        CN3[ControlNode Follower]
        CN1 <-->|Raft Consensus| CN2
        CN1 <-->|Raft Consensus| CN3
    end

    subgraph Data Plane
        DN1[DataNode 1]
        DN2[DataNode 2]
        DN3[DataNode 3]
    end

    CLI <-->|1. Metadata/Auth gRPC| CN1
    CLI <-->|2. Block Transfer gRPC| DN1
    CLI <-->|2. Block Transfer gRPC| DN2
    
    DN1 <-->|Heartbeats/Block Reports| CN1
    DN2 <-->|Heartbeats/Block Reports| CN1
    DN3 <-->|Heartbeats/Block Reports| CN1
    
    DN1 <-->|Block Replication| DN2
    DN2 <-->|Block Replication| DN3
:::