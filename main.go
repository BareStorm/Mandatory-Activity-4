package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	pb "ricart/proto"

	"google.golang.org/grpc"
)

// parsePeers turns "2=localhost:5002,3=localhost:5003" into {2: "localhost:5002", ...}
func parsePeers(s string) (map[int32]string, error) {
	peers := make(map[int32]string)
	if s == "" {
		return peers, nil
	}
	for _, entry := range strings.Split(s, ",") {
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("bad peer entry %q, expected id=host:port", entry)
		}
		id, err := strconv.Atoi(parts[0])
		if err != nil {
			return nil, fmt.Errorf("bad peer id %q: %w", parts[0], err)
		}
		peers[int32(id)] = parts[1]
	}
	return peers, nil
}

func main() {
	id := flag.Int("id", 0, "this node's unique, non-zero ID")
	addr := flag.String("addr", "localhost:5001", "address this node listens on")
	peersFlag := flag.String("peers", "", "other nodes as id=host:port, comma-separated")
	auto := flag.Bool("auto", false, "request the critical section at random intervals")
	flag.Parse()
	log.SetFlags(0) // logs are [id | t=clock] only, no wall-clock prefix

	if *id == 0 {
		log.Fatal("you must supply a unique non-zero -id")
	}

	peerAddrs, err := parsePeers(*peersFlag)
	if err != nil {
		log.Fatal(err)
	}

	node := &Node{ID: int32(*id), Peers: make(map[int32]pb.MutualExclusionClient)}

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer()
	pb.RegisterMutualExclusionServer(server, node)
	go func() {
		log.Printf("[node %d] listening on %s", node.ID, *addr)
		if err := server.Serve(lis); err != nil {
			log.Fatalf("serve: %v", err)
		}
	}()

	if err := node.connectToPeers(peerAddrs); err != nil {
		log.Fatal(err)
	}

	if *auto {
		for {
			time.Sleep(time.Duration(1000+rand.Intn(3000)) * time.Millisecond)
			node.RequestCriticalSection()
		}
	}
	fmt.Println("press Enter to request the critical section")
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		go node.RequestCriticalSection()
	}
	select {}
}
