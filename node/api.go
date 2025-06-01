package node

import (
		"github.com/saiweb3dev/distributed_consensus_simulator/types"
)

// GetID returns the node's ID
func (n *RaftNode) GetID() int {
	return n.id
}

// GetState returns the current state of the node (thread-safe)
func (n *RaftNode) GetState() (types.NodeState, int, int) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.state, n.currentTerm, n.currentLeader
}

// GetNodeInfo returns comprehensive node information
func (n *RaftNode) GetNodeInfo() types.NodeInfo {
	n.mu.RLock()
	defer n.mu.RUnlock()
	
	return types.NodeInfo{
		ID:            n.id,
		State:         n.state,
		Term:          n.currentTerm,
		Leader:        n.currentLeader,
		LogLength:     len(n.log),
		CommitIndex:   n.commitIndex,
		LastHeartbeat: n.lastHeartbeat,
	}
}

// SendMessage sends a message to this node (implements NodeInterface)
func (n *RaftNode) SendMessage(msg types.Message) {
	select {
	case n.inbox <- msg:
	default:
		// Inbox full - this could happen under high load
		// In production, you might want to handle this differently
	}
}

// SubmitCommand submits a command to the cluster (client interface)
func (n *RaftNode) SubmitCommand(command interface{}) error {
	n.mu.RLock()
	isLeader := n.state == types.Leader
	n.mu.RUnlock()
	
	if !isLeader {
		return ErrNotLeader
	}
	
	// Create client request message
	msg := types.Message{
		Type:    types.ClientRequest,
		From:    -1, // -1 indicates client request
		To:      n.id,
		Command: command,
	}
	
	n.SendMessage(msg)
	return nil
}

// IsLeader returns true if this node is the current leader
func (n *RaftNode) IsLeader() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.state == types.Leader
}

// GetLeader returns the current leader ID (-1 if unknown)
func (n *RaftNode) GetLeader() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.currentLeader
}

// GetLogLength returns the length of the node's log
func (n *RaftNode) GetLogLength() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return len(n.log)
}

// Stop gracefully stops the node
func (n *RaftNode) Stop() {
	close(n.stopCh)
}