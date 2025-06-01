package main

import (
	"fmt"
	"log"
	"time"
	"github.com/saiweb3dev/distributed_consensus_simulator/node"
	"github.com/saiweb3dev/distributed_consensus_simulator/types"
	"github.com/saiweb3dev/distributed_consensus_simulator/network"
	"github.com/saiweb3dev/distributed_consensus_simulator/client"
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
    // Set cleaner logging
    log.SetFlags(log.Ltime)
    
    fmt.Println("🚀 Starting Distributed Banking System with Raft Consensus")
    
    // Create network simulator with less noise
    networkConfig := types.DefaultNetworkConfig()
    networkConfig.BaseLatency = 5 * time.Millisecond // Faster for demo
    networkSim := network.NewNetworkSimulator(networkConfig)
    defer networkSim.Stop()
    
    // Create node registry and router
    registry := NewSimpleNodeRegistry()
    router := network.NewClusterRouter(networkSim, registry)
    
    // Create 3-node cluster
    nodeConfig := types.DefaultNodeConfig()
    nodeConfig.HeartbeatInterval = 100 * time.Millisecond // Less frequent heartbeats
    
    nodes := []*node.RaftNode{
        node.NewRaftNode(0, []int{1, 2}, nodeConfig, router),
        node.NewRaftNode(1, []int{0, 2}, nodeConfig, router),
        node.NewRaftNode(2, []int{0, 1}, nodeConfig, router),
    }
    
    // Register and start nodes
    for _, n := range nodes {
        registry.AddNode(n)
        n.Start()
    }
    
    fmt.Println("⏳ Waiting for leader election...")
    time.Sleep(1 * time.Second)
    
    // Create clients
    clientA := client.NewClient("ClientA", nodes)
    clientB := client.NewClient("ClientB", nodes)
    
    fmt.Println("\n💰 Setting up banking system...")
    
    // Setup phase - create accounts
    accounts := []struct{ name string; balance float64 }{
        {"alice", 1000.0},
        {"bob", 1500.0},
        {"charlie", 800.0},
    }
    
    for _, acc := range accounts {
        err := clientA.CreateAccount(acc.name, acc.balance)
        if err != nil {
            fmt.Printf("❌ Failed to create account %s: %v\n", acc.name, err)
        } else {
            fmt.Printf("✅ Created account %s with $%.2f\n", acc.name, acc.balance)
        }
        time.Sleep(100 * time.Millisecond)
    }
    
    fmt.Println("\n💸 Performing transactions...")
    
    // Demonstrate various operations
    transactions := []func(){
        func() {
            clientA.Transfer("alice", "bob", 100.0)
            fmt.Println("🔄 Alice -> Bob: $100")
        },
        func() {
            clientB.Transfer("bob", "charlie", 250.0)
            fmt.Println("🔄 Bob -> Charlie: $250")
        },
        func() {
            clientA.Set("last_transaction", "charlie_deposit")
            fmt.Println("📝 Set metadata: last_transaction")
        },
        func() {
            clientB.Transfer("charlie", "alice", 50.0)
            fmt.Println("🔄 Charlie -> Alice: $50")
        },
    }
    
    for i, txn := range transactions {
        txn()
        time.Sleep(200 * time.Millisecond)
        
        // Show balances after some transactions
        if i == 1 {
            fmt.Println("\n📊 Current Balances:")
            for _, acc := range accounts {
                if balance, err := clientA.GetBalance(acc.name); err == nil {
                    fmt.Printf("   %s: $%.2f\n", acc.name, balance)
                }
            }
            fmt.Println()
        }
    }
    
    fmt.Println("\n🔄 Simulating network partition...")
    router.SetNetworkPartition([]int{2}) // Isolate node 2
    
    // Continue operations during partition
    clientA.Transfer("alice", "bob", 75.0)
    fmt.Println("🔄 During partition: Alice -> Bob: $75")
    
    time.Sleep(1 * time.Second)
    
    fmt.Println("🔧 Healing network partition...")
    router.SetNetworkPartition([]int{}) // Heal partition
    
    time.Sleep(500 * time.Millisecond)
    
    // Operation after healing
    clientB.Transfer("bob", "alice", 25.0)
    fmt.Println("🔄 After healing: Bob -> Alice: $25")
    
    time.Sleep(500 * time.Millisecond)
    
    // Final state
    fmt.Println("\n📋 Final System State:")
    fmt.Println("======================")
    
    // Show final balances
    fmt.Println("💰 Account Balances:")
    for _, acc := range accounts {
        if balance, err := clientA.GetBalance(acc.name); err == nil {
            fmt.Printf("   %-8s: $%8.2f\n", acc.name, balance)
        }
    }
    
    // Show cluster state
    fmt.Println("\n🖥️  Cluster State:")
    for _, n := range nodes {
        info := n.GetNodeInfo()
        fmt.Printf("   Node %d: %s (Term: %d, Log: %d entries)\n",
            info.ID, info.State, info.Term, info.LogLength)
    }
    
    // Show network stats
    sent, delivered, dropped := router.GetNetworkStats()
    fmt.Printf("\n📡 Network: %d sent, %d delivered, %d dropped\n", sent, delivered, dropped)
    
    // Cleanup
    for _, n := range nodes {
        n.Stop()
    }
    
    fmt.Println("\n✅ Banking system demonstration completed!")
}