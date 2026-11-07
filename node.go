package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	pb "ricart/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type State int

const (
	Released State = iota
	Wanted
	Held
)

func (s State) String() string {
	return [...]string{"RELEASED", "WANTED", "HELD"}[s]
}

type Node struct {
	pb.UnimplementedMutualExclusionServer

	ID    int32
	Peers map[int32]pb.MutualExclusionClient // one client per other node

	mu			sync.Mutex		//guard
	state		State
	clock		int64			//Lamport clock
	reqTS		int64			//timestamp of own current request (while WANTED)
	deffered	[]chan struct{} //one channel per deffered incoming request

}

// logf prints in the agreed format. Caller MUST hold n.mu (it reads n.clock).
func (n *Node) logf(format string, args ...any) {
	log.Printf("[%d | t=%d] %s", n.ID, n.clock, fmt.Sprintf(format, args...))
}

// RequestAccess is the server side of the algorithm. PART 2 implements this.
// Hold the call open to defer the reply; return when permission is granted.
func (n *Node) RequestAccess(ctx context.Context, req *pb.Request) (*pb.Reply, error) {
	n.mu.Lock()
	n.clock = max(n.clock, req.Timestamp) + 1 // Lamport receive rule
	n.logf("received REQUEST from %d (ts=%d), my state=%s", req.NodeId, req.Timestamp, n.state)
 
	// (reqTS, ID) lexicographic: lower pair has priority.
	weWin := n.reqTS < req.Timestamp || (n.reqTS == req.Timestamp && n.ID < req.NodeId)
	if n.state == Held || (n.state == Wanted && weWin) {
		ch := make(chan struct{})
		n.deferred = append(n.deferred, ch)
		n.logf("DEFERRING reply to %d", req.NodeId)
		n.mu.Unlock() // never block while holding the mutex
 
		select {
		case <-ch:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return &pb.Reply{NodeId: n.ID}, nil
	}

	n.logf("replying to %d immediately", req.NodeId)
	n.mu.Unlock()
	return &pb.Reply{NodeId: n.ID}, nil
}

// RequestCriticalSection runs one full entry -> CS -> exit cycle.
func (n *Node) RequestCriticalSection() {
	n.mu.Lock()
	if n.state != Released { // already wanting or inside: ignore duplicate trigger
		n.mu.Unlock()
		return
	}
	n.state = Wanted
	n.clock++
	n.reqTS = n.clock
	ts := n.reqTS
	n.logf("WANTED critical section, sending REQUEST(ts=%d) to all peers", ts)
	n.mu.Unlock()
 
	// Ask every peer in parallel; each call returns only when that peer replies.
	var wg sync.WaitGroup
	errs := make(chan error, len(n.Peers))
	for id, peer := range n.Peers {
		wg.Add(1)
		go func(id int32, peer pb.MutualExclusionClient) {
			defer wg.Done()
			reply, err := peer.RequestAccess(context.Background(),
				&pb.Request{Timestamp: ts, NodeId: n.ID}, grpc.WaitForReady(true))
			if err != nil {
				errs <- fmt.Errorf("peer %d: %w", id, err)
				return
			}
			n.mu.Lock()
			n.logf("received REPLY from %d", reply.NodeId)
			n.mu.Unlock()
		}(id, peer)
	}
	wg.Wait()
	close(errs)
	if err := <-errs; err != nil {
		log.Printf("[%d] request failed: %v", n.ID, err)
		n.release()
		return
	}
 
	n.mu.Lock()
	n.state = Held
	n.logf("ENTERING critical section (HELD)")
	n.mu.Unlock()
 
	fmt.Printf(">>> node %d is in the CRITICAL SECTION <<<\n", n.ID)
	time.Sleep(2 * time.Second)
 
	n.release()
}
 
// release leaves the CS and grants all deferred replies.
func (n *Node) release() {
	n.mu.Lock()
	n.state = Released
	pending := n.deferred
	n.deferred = nil
	n.logf("EXIT critical section (RELEASED), sending %d deferred replies", len(pending))
	n.mu.Unlock()
 
	for _, ch := range pending {
		close(ch) // unblocks the waiting handlers; each channel is closed exactly once
	}
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
