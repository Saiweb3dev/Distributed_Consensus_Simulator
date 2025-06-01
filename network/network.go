package network

import (
	"log"
	"math/rand"
	"sync"
	"time"
	"github.com/saiweb3dev/distributed_consensus_simulator/types"
)

// NetworkSimulator simulates network conditions like latency, packet loss, and partitions
type NetworkSimulator struct {
	config types.NetworkConfig
	mu     sync.RWMutex
	
	// Message delivery
	deliveryQueue chan delayedMessage
	stopCh        chan struct{}
	
	// Statistics
	messagesSent     uint64
	messagesDelivered uint64
	messagesDropped   uint64
}

type delayedMessage struct {
	message     types.Message
	deliverTime time.Time
	router      MessageRouter
}

// MessageRouter interface for delivering messages to nodes
type MessageRouter interface {
	DeliverMessage(msg types.Message)
}

// NewNetworkSimulator creates a new network simulator
func NewNetworkSimulator(config types.NetworkConfig) *NetworkSimulator {
	ns := &NetworkSimulator{
		config:        config,
		deliveryQueue: make(chan delayedMessage, 1000),
		stopCh:        make(chan struct{}),
	}
	
	if config.Enabled {
		go ns.deliveryLoop()
	}
	
	return ns
}

// SendMessage simulates sending a message through the network
func (ns *NetworkSimulator) SendMessage(msg types.Message, router MessageRouter) {
	ns.mu.Lock()
	ns.messagesSent++
	ns.mu.Unlock()
	
	if !ns.config.Enabled {
		// Direct delivery if simulation disabled
		router.DeliverMessage(msg)
		return
	}
	
	// Check for packet loss
	if rand.Float64() < ns.config.PacketLoss {
		ns.mu.Lock()
		ns.messagesDropped++
		ns.mu.Unlock()
		log.Printf("Network: Dropped message %s from %d to %d", 
			msg.Type, msg.From, msg.To)
		return
	}
	
	// Check for network partition
	if ns.isPartitioned(msg.From, msg.To) {
		ns.mu.Lock()
		ns.messagesDropped++
		ns.mu.Unlock()
		log.Printf("Network: Partitioned message %s from %d to %d", 
			msg.Type, msg.From, msg.To)
		return
	}
	
	// Calculate delivery delay
	delay := ns.calculateDelay()
	msg.Timestamp = time.Now()
	msg.Delay = delay
	
	deliverTime := time.Now().Add(delay)
	
	select {
	case ns.deliveryQueue <- delayedMessage{
		message:     msg,
		deliverTime: deliverTime,
		router:      router,
	}:
	default:
		log.Printf("Network: Delivery queue full, dropping message")
		ns.mu.Lock()
		ns.messagesDropped++
		ns.mu.Unlock()
	}
}

// deliveryLoop handles delayed message delivery
func (ns *NetworkSimulator) deliveryLoop() {
	ticker := time.NewTicker(1 * time.Millisecond)
	defer ticker.Stop()
	
	var pendingMessages []delayedMessage
	
	for {
		select {
		case msg := <-ns.deliveryQueue:
			pendingMessages = append(pendingMessages, msg)
			
		case <-ticker.C:
			now := time.Now()
			var remaining []delayedMessage
			
			for _, msg := range pendingMessages {
				if now.After(msg.deliverTime) {
					// Deliver message
					msg.router.DeliverMessage(msg.message)
					ns.mu.Lock()
					ns.messagesDelivered++
					ns.mu.Unlock()
				} else {
					remaining = append(remaining, msg)
				}
			}
			
			pendingMessages = remaining
			
		case <-ns.stopCh:
			return
		}
	}
}

// calculateDelay returns network delay with jitter
func (ns *NetworkSimulator) calculateDelay() time.Duration {
	baseDelay := ns.config.BaseLatency
	jitter := time.Duration(rand.Int63n(int64(ns.config.LatencyJitter)))
	return baseDelay + jitter
}

// isPartitioned checks if two nodes are partitioned
func (ns *NetworkSimulator) isPartitioned(from, to int) bool {
	ns.mu.RLock()
	defer ns.mu.RUnlock()
	
	partitioned := make(map[int]bool)
	for _, nodeID := range ns.config.PartitionNodes {
		partitioned[nodeID] = true
	}
	
	// If one node is partitioned and the other isn't, drop message
	fromPartitioned := partitioned[from]
	toPartitioned := partitioned[to]
	
	return fromPartitioned != toPartitioned
}

// SetPartition sets which nodes are partitioned
func (ns *NetworkSimulator) SetPartition(nodeIDs []int) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	ns.config.PartitionNodes = nodeIDs
	log.Printf("Network: Set partition for nodes %v", nodeIDs)
}

// GetStats returns network statistics
func (ns *NetworkSimulator) GetStats() (uint64, uint64, uint64) {
	ns.mu.RLock()
	defer ns.mu.RUnlock()
	return ns.messagesSent, ns.messagesDelivered, ns.messagesDropped
}

// Stop stops the network simulator
func (ns *NetworkSimulator) Stop() {
	close(ns.stopCh)
}