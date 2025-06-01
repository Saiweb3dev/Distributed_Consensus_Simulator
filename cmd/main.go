package main

import (
	"fmt"
	"log"
	"time"
	"github.com/saiweb3dev/distributed_consensus_simulator/node"
	"github.com/saiweb3dev/distributed_consensus_simulator/types"
	"github.com/saiweb3dev/distributed_consensus_simulator/network"
)
// SimpleNodeRegistry implements NodeRegistry for the demo
type SimpleNodeRegistry struct {
	nodes map[int]*node.RaftNode
}

func NewSimpleNodeRegistry() *SimpleNodeRegistry {
	return &SimpleNodeRegistry{
		nodes: make(map[int]*node.RaftNode),
	}
}

func (r *SimpleNodeRegistry) AddNode(n *node.RaftNode) {
	r.nodes[n.GetID()] = n
}

func (r *SimpleNodeRegistry) GetNode(id int) (network.NodeInterface, bool) {
	node, exists := r.nodes[id]
	return node, exists
}

func (r *SimpleNodeRegistry) GetAllNodes() []network.NodeInterface {
	var nodes []network.NodeInterface
	for _, n := range r.nodes {
		nodes = append(nodes, n)
	}
	return nodes
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	
	fmt.Println("=== Starting Raft Cluster with Network Simulation ===")
	
	// Create network simulator
	networkConfig := types.DefaultNetworkConfig()
	networkSim := network.NewNetworkSimulator(networkConfig)
	defer networkSim.Stop()
	
	// Create node registry
	registry := NewSimpleNodeRegistry()
	
	// Create cluster router
	router := network.NewClusterRouter(networkSim, registry)
	
	// Create nodes
	nodeConfig := types.DefaultNodeConfig()
	node0 := node.NewRaftNode(0, []int{1, 2}, nodeConfig, router)
	node1 := node.NewRaftNode(1, []int{0, 2}, nodeConfig, router)
	node2 := node.NewRaftNode(2, []int{0, 1}, nodeConfig, router)
	
	// Register nodes
	registry.AddNode(node0)
	registry.AddNode(node1)
	registry.AddNode(node2)
	
	nodes := []*node.RaftNode{node0, node1, node2}
	
	// Start all nodes
	for _, n := range nodes {
		n.Start()
	}
	
	// Monitor cluster for 15 seconds
	for i := 0; i < 15; i++ {
		fmt.Printf("\n=== Time: %d seconds ===\n", i)
		
		// Print node states
		for _, n := range nodes {
			info := n.GetNodeInfo()
			fmt.Printf("Node %d: %s (Term: %d, Leader: %d, Log: %d entries)\n",
				info.ID, info.State, info.Term, info.Leader, info.LogLength)
		}
		
		// Print network stats
		sent, delivered, dropped := router.GetNetworkStats()
		fmt.Printf("Network: Sent=%d, Delivered=%d, Dropped=%d\n", sent, delivered, dropped)
		
		// Simulate network partition at 5 seconds
		if i == 5 {
			fmt.Println("*** Creating network partition (isolating node 2) ***")
			router.SetNetworkPartition([]int{2})
		}
		
		// Heal partition at 10 seconds
		if i == 10 {
			fmt.Println("*** Healing network partition ***")
			router.SetNetworkPartition([]int{})
		}
		
		time.Sleep(1 * time.Second)
	}
	
	// Stop all nodes
	for _, n := range nodes {
		n.Stop()
	}
	
	fmt.Println("\n=== Cluster Stopped ===")
}