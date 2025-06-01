package node

import (
	"errors"
	"math/rand"
	"time"
	"log"
	"github.com/saiweb3dev/distributed_consensus_simulator/types"
)

// Common errors
var (
	ErrNotLeader = errors.New("node is not the leader")
	ErrStopped   = errors.New("node is stopped")
)

// becomeLeader transitions the node to leader state
func (n *RaftNode) becomeLeader() {
    n.state = types.Leader
    n.currentLeader = n.id
    
    // Initialize leader state for log replication
    nextIndex := len(n.log)
    for i := range n.nextIndex {
        n.nextIndex[i] = nextIndex
        n.matchIndex[i] = -1
    }
    
    // Stop election timer and start heartbeat ticker
    n.electionTimer.Stop()
    n.heartbeatTicker = time.NewTicker(n.config.HeartbeatInterval)
    
    // Send immediate heartbeat to establish leadership
    n.sendHeartbeats()
    
    log.Printf("Node %d became leader for term %d", n.id, n.currentTerm)
}

// appendLogEntry adds a new entry to the log
func (n *RaftNode) appendLogEntry(command interface{}) int {
    entry := types.LogEntry{
        Term:    n.currentTerm,
        Index:   len(n.log),
        Command: command,
    }
    n.log = append(n.log, entry)
    log.Printf("Node %d appended log entry at index %d: %v", n.id, entry.Index, command)
    return entry.Index
}

// replicateToFollower sends append entries to a specific follower
func (n *RaftNode) replicateToFollower(followerID int) {
    if n.state != types.Leader {
        return
    }
    
    // Find follower index in peers slice
    followerIndex := -1
    for i, peer := range n.peers {
        if peer == followerID {
            followerIndex = i
            break
        }
    }
    
    if followerIndex == -1 {
        return
    }
    
    nextIndex := n.nextIndex[followerIndex]
    prevLogIndex := nextIndex - 1
    prevLogTerm := 0
    
    // Get previous log term if it exists
    if prevLogIndex >= 0 && prevLogIndex < len(n.log) {
        prevLogTerm = n.log[prevLogIndex].Term
    }
    
    // Prepare entries to send (from nextIndex onwards)
    var entries []types.LogEntry
    if nextIndex < len(n.log) {
        entries = make([]types.LogEntry, len(n.log)-nextIndex)
        copy(entries, n.log[nextIndex:])
    }
    
    msg := types.Message{
        Type:         types.AppendEntries,
        From:         n.id,
        To:           followerID,
        Term:         n.currentTerm,
        PrevLogIndex: prevLogIndex,
        PrevLogTerm:  prevLogTerm,
        Entries:      entries,
        LeaderCommit: n.commitIndex,
    }
    
    n.sendMessageViaRouter(msg)
    log.Printf("Leader %d sent %d entries to follower %d (nextIndex: %d)", 
        n.id, len(entries), followerID, nextIndex)
}

// updateCommitIndex updates the commit index based on majority replication
func (n *RaftNode) updateCommitIndex() {
    if n.state != types.Leader {
        return
    }
    
    // Find highest index replicated on majority of servers
    for i := len(n.log) - 1; i > n.commitIndex; i-- {
        if n.log[i].Term != n.currentTerm {
            continue // Only commit entries from current term
        }
        
        // Count replicas (including leader)
        replicaCount := 1 // Leader has it
        for j, _ := range n.peers {
            if n.matchIndex[j] >= i {
                replicaCount++
            }
        }
        
        majority := (len(n.peers)+1)/2 + 1
        if replicaCount >= majority {
            oldCommitIndex := n.commitIndex
            n.commitIndex = i
            log.Printf("Leader %d updated commit index from %d to %d", 
                n.id, oldCommitIndex, n.commitIndex)
            
            // Apply newly committed entries
            n.applyCommittedEntries()
            break
        }
    }
}

// applyCommittedEntries applies committed entries to state machine
func (n *RaftNode) applyCommittedEntries() {
    for n.lastApplied < n.commitIndex {
        n.lastApplied++
        entry := n.log[n.lastApplied]
        
        // Apply to state machine
        if cmd, ok := entry.Command.(map[string]interface{}); ok {
            if op, exists := cmd["op"]; exists && op == "set" {
                if key, keyOk := cmd["key"].(string); keyOk {
                    if value, valueOk := cmd["value"]; valueOk {
                        n.stateMachine[key] = value
                        log.Printf("Node %d applied: SET %s = %v", n.id, key, value)
                    }
                }
            }
        }
        
        // Notify pending client if this was our command
        if respCh, exists := n.pendingCommands[n.lastApplied]; exists {
            close(respCh)
            delete(n.pendingCommands, n.lastApplied)
        }
    }
}

// getLastLogInfo returns the index and term of the last log entry
func (n *RaftNode) getLastLogInfo() (int, int) {
	if len(n.log) == 0 {
		return -1, 0
	}
	lastEntry := n.log[len(n.log)-1]
	return lastEntry.Index, lastEntry.Term
}

// randomElectionTimeout generates a random election timeout
func (n *RaftNode) randomElectionTimeout() time.Duration {
	min := n.config.ElectionTimeoutMin
	max := n.config.ElectionTimeoutMax
	return min + time.Duration(rand.Int63n(int64(max-min)))
}

// resetElectionTimer resets the election timeout timer
func (n *RaftNode) resetElectionTimer() {
	if !n.electionTimer.Stop() {
		select {
		case <-n.electionTimer.C:
		default:
		}
	}
	n.electionTimer.Reset(n.randomElectionTimeout())
}

// sendMessageViaRouter sends a message through the cluster router
func (n *RaftNode) sendMessageViaRouter(msg types.Message) {
	if n.router != nil {
		n.router.RouteMessage(msg)
	}
}

// broadcastToAllPeers sends a message to all peer nodes
func (n *RaftNode) broadcastToAllPeers(msgType types.MessageType, extraData map[string]interface{}) {
	if n.router == nil {
		return
	}
	
	lastLogIndex, lastLogTerm := n.getLastLogInfo()
	
	for _, peer := range n.peers {
		msg := types.Message{
			Type:         msgType,
			From:         n.id,
			To:           peer,
			Term:         n.currentTerm,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}
		
		// Add extra data based on message type
		if msgType == types.VoteRequest {
			msg.CandidateID = n.id
		}
		
		n.sendMessageViaRouter(msg)
	}
}

// stepDown converts the node to follower state
func (n *RaftNode) stepDown(newTerm int) {
	oldState := n.state
	
	n.currentTerm = newTerm
	n.votedFor = -1
	n.state = types.Follower
	n.currentLeader = -1
	
	// Stop heartbeat ticker if we were leader
	if oldState == types.Leader {
		n.heartbeatTicker.Stop()
	}
	
	n.resetElectionTimer()
}

// becomeCandidate transitions the node to candidate state
func (n *RaftNode) becomeCandidate() {
	n.state = types.Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.currentLeader = -1
	n.resetElectionTimer()
	
	// Reset vote tracking
	n.votesReceived = make(map[int]bool)
	n.votesReceived[n.id] = true // Vote for ourselves
}


// sendHeartbeats sends heartbeat messages to all followers
func (n *RaftNode) sendHeartbeats() {
	for _, peer := range n.peers {
		msg := types.Message{
			Type: types.Heartbeat,
			From: n.id,
			To:   peer,
			Term: n.currentTerm,
		}
		n.sendMessageViaRouter(msg)
	}
}

// hasMajority checks if we have received votes from a majority of nodes
func (n *RaftNode) hasMajority() bool {
	totalNodes := len(n.peers) + 1 // +1 for self
	majority := totalNodes/2 + 1
	return len(n.votesReceived) >= majority
}

// isLogUpToDate checks if candidate's log is at least as up-to-date as ours
func (n *RaftNode) isLogUpToDate(candidateLastIndex, candidateLastTerm int) bool {
	ourLastIndex, ourLastTerm := n.getLastLogInfo()
	
	// Candidate's log is more up-to-date if it has a higher term
	if candidateLastTerm > ourLastTerm {
		return true
	}
	
	// If terms are equal, candidate's log is more up-to-date if it's longer
	if candidateLastTerm == ourLastTerm && candidateLastIndex >= ourLastIndex {
		return true
	}
	
	return false
}

func min(a, b int) int {
    if a < b {
        return a
    }
    return b
}