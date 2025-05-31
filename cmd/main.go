package main

import (
	"fmt"
	"log"
	"time"
	"github.com/saiweb3dev/distributed_consensus_simulator/node"
	"github.com/saiweb3dev/distributed_consensus_simulator/types"
)
func main(){
log.SetFlags(log.LstdFlags | log.Lshortfile)
	
	// Create a 3-node cluster
	config := types.DefaultNodeConfig()
	
	// Node 0
	node0 := node.NewRaftNode(0, []int{1, 2}, config)
	// Node 1  
	node1 := node.NewRaftNode(1, []int{0, 2}, config)
	// Node 2
	node2 := node.NewRaftNode(2, []int{0, 1}, config)
	
	nodes := []*node.RaftNode{node0, node1, node2}

		// Start all nodes
	for _, n := range nodes {
		n.Start()
	}
	
	fmt.Println("=== Raft Cluster Started ===")


		// Simple message routing simulation
	go func() {
		for {
			for _, sender := range nodes {
				if msg, ok := sender.GetOutboxMessage(); ok {
					// Route message to recipient
					for _, receiver := range nodes {
						state, _, leader := receiver.GetState()
						if state == types.Follower && msg.To == leader {
							log.Printf("Node %d sending message to Node %d: %s\n", getNodeID(sender), getNodeID(receiver), msg.Type)
							break
						}
					}
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

		// Monitor cluster state
	for i := 0; i < 10; i++ {
		fmt.Printf("\n=== Time: %d seconds ===\n", i)
		for _, n := range nodes {
			state, term, leader := n.GetState()
			fmt.Printf("Node %d: State=%s, Term=%d, Leader=%d\n", 
				getNodeID(n), state, term, leader)
		}
		time.Sleep(1 * time.Second)
	}

// Stop all nodes
	for _, n := range nodes {
		n.Stop()
	}
	
	fmt.Println("=== Cluster Stopped ===")
}

// Helper function to get node ID 
func getNodeID(n *node.RaftNode) int {
	// This is a simplified way to identify nodes for the demo
	// In practice, we'd have a proper getter method
	return 0 // Placeholder
}