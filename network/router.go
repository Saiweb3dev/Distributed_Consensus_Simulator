package network

import (
	"log"
	"sync"
	"github.com/saiweb3dev/distributed_consensus_simulator/types"
)


// NodeRegistry interface for finding nodes
type NodeRegistry interface {
	GetNode(id int) (NodeInterface, bool)
	GetAllNodes() []NodeInterface
}

// NodeInterface represents the interface a node must implement for routing
type NodeInterface interface {
	GetID() int
	SendMessage(msg types.Message)
}

// ClusterRouter handles message routing between nodes in a cluster
type ClusterRouter struct {
	network  *NetworkSimulator
	registry NodeRegistry
	mu       sync.RWMutex
}

// NewClusterRouter creates a new cluster message router
func NewClusterRouter(network *NetworkSimulator, registry NodeRegistry) *ClusterRouter {
	return &ClusterRouter{
		network:  network,
		registry: registry,
	}
}

// RouteMessage routes a message from one node to another
func (cr *ClusterRouter) RouteMessage(msg types.Message) {
	if msg.To == msg.From {
		log.Printf("Router: Ignoring self-message from node %d", msg.From)
		return
	}
	
	// Use network simulator to send message
	cr.network.SendMessage(msg, cr)
}

// DeliverMessage implements MessageRouter interface
func (cr *ClusterRouter) DeliverMessage(msg types.Message) {
	targetNode, exists := cr.registry.GetNode(msg.To)
	if !exists {
		log.Printf("Router: Target node %d not found", msg.To)
		return
	}
	
	log.Printf("Router: Delivering %s from %d to %d (delay: %v)", 
		msg.Type, msg.From, msg.To, msg.Delay)
	
	targetNode.SendMessage(msg)
}

// BroadcastMessage sends a message to multiple nodes
func (cr *ClusterRouter) BroadcastMessage(msg types.Message, targets []int) {
	for _, target := range targets {
		broadcastMsg := msg
		broadcastMsg.To = target
		cr.RouteMessage(broadcastMsg)
	}
}

// GetNetworkStats returns network simulation statistics
func (cr *ClusterRouter) GetNetworkStats() (uint64, uint64, uint64) {
	return cr.network.GetStats()
}

// SetNetworkPartition creates a network partition
func (cr *ClusterRouter) SetNetworkPartition(nodeIDs []int) {
	cr.network.SetPartition(nodeIDs)
}