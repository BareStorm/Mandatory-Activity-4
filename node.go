package main

import (
	"context"
	"fmt"
	"log"

	pb "ricart/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Node struct {
	pb.UnimplementedMutualExclusionServer

	ID    int32
	Peers map[int32]pb.MutualExclusionClient // one client per other node

	// PART 2 adds here: mutex, state (RELEASED/WANTED/HELD),
	// lamport clock, own request timestamp, deferred-reply mechanism.
}

// RequestAccess is the server side of the algorithm. PART 2 implements this.
// Hold the call open to defer the reply; return when permission is granted.
func (n *Node) RequestAccess(ctx context.Context, req *pb.Request) (*pb.Reply, error) {
	return nil, fmt.Errorf("not implemented")
}

func (n *Node) connectToPeers(addrs map[int32]string) error {
	for id, addr := range addrs {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("connecting to node %d at %s: %w", id, addr, err)
		}
		n.Peers[id] = pb.NewMutualExclusionClient(conn)
		log.Printf("[node %d] registered peer %d at %s", n.ID, id, addr)
	}
	return nil
}
