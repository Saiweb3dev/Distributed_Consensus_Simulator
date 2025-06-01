/*
Core node implementation with message processing
*/
package node

import (
	"log"
	"sync"
	"time"
	"github.com/saiweb3dev/distributed_consensus_simulator/types"
)

// MessageRouter interface for sending messages
type MessageRouter interface {
	RouteMessage(msg types.Message)
}

// RaftNode represents a single node in the Raft cluster
type RaftNode struct {
	// Node identification
	id     int
	peers  []int
	config types.NodeConfig

	// Persistent state (would be stored on disk in real implementation)
	currentTerm int              // Latest term server has seen
	votedFor    int              // CandidateId that received vote in current term (-1 if none)
	log         []types.LogEntry // Log entries

	// Volatile state
	commitIndex int // Index of highest log entry known to be committed
	lastApplied int // Index of highest log entry applied to state machine

	// Leader state (reinitialized after election)
	nextIndex  []int // For each server, index of next log entry to send
	matchIndex []int // For each server, index of highest log entry known to be replicated

	// Node state management
	state         types.NodeState
	mu            sync.RWMutex // Protects all shared state
	currentLeader int          // ID of current leader (-1 if unknown)
	votesReceived map[int]bool // Track votes received in current election

	// Communication channels
	inbox  chan types.Message // Incoming messages
	stopCh chan struct{}      // Signal to stop the node
	router MessageRouter // Router for sending messages

	// Timing
	electionTimer   *time.Timer
	heartbeatTicker *time.Ticker
	lastHeartbeat   time.Time

	// Add for log replication
    stateMachine map[string]interface{} // Simple key-value state machine
    pendingCommands map[int]chan error  // Track pending client commands
}

// NewRaftNode creates a new Raft node
func NewRaftNode(id int, peers []int, config types.NodeConfig, router MessageRouter) *RaftNode {
    node := &RaftNode{
        id:            id,
        peers:         peers,
        config:        config,
        router:        router,
        currentTerm:   0,
        votedFor:      -1,
        log:           make([]types.LogEntry, 0),
        commitIndex:   -1, // -1 indicates no entries committed yet
        lastApplied:   -1,
        state:         types.Follower,
        currentLeader: -1,
        votesReceived: make(map[int]bool),
        inbox:         make(chan types.Message, 100),
        stopCh:        make(chan struct{}),
        lastHeartbeat: time.Now(),
				stateMachine:  make(map[string]interface{}),
        pendingCommands: make(map[int]chan error),
    }

    // Initialize leader state
    peerCount := len(peers) + 1 // +1 for self
    node.nextIndex = make([]int, peerCount)
    node.matchIndex = make([]int, peerCount)

    // Initialize timers
    node.electionTimer = time.NewTimer(node.randomElectionTimeout())

    return node
}

// sendMessage sends a message through the router
func (n *RaftNode) sendMessage(msg types.Message) {
    if n.router != nil {
        n.router.RouteMessage(msg)
    }
}

// Start begins the node's main processing loop
func (n *RaftNode) Start() {
	log.Printf("Node %d starting in %s state", n.id, n.state)
	go n.messageLoop()
}

// messageLoop is the main event loop for processing messages and timeouts
func (n *RaftNode) messageLoop() {
    for {
        select {
        case msg := <-n.inbox:
            n.handleMessage(msg)

        case <-n.electionTimer.C:
            n.handleElectionTimeout()

        case <-func() <-chan time.Time {
            if n.heartbeatTicker != nil {
                return n.heartbeatTicker.C
            }
            return make(chan time.Time) // Return a channel that never receives
        }():
            n.handleHeartbeatTimeout()

        case <-n.stopCh:
            log.Printf("Node %d stopping", n.id)
            n.electionTimer.Stop()
            if n.heartbeatTicker != nil {
                n.heartbeatTicker.Stop()
            }
            return
        }
    }
}

// handleMessage processes incoming messages based on type and current state
func (n *RaftNode) handleMessage(msg types.Message) {
	n.mu.Lock()
	defer n.mu.Unlock()

	log.Printf("Node %d received %s from Node %d (term: %d, current term: %d)", 
		n.id, msg.Type, msg.From, msg.Term, n.currentTerm)

	// If message term is higher, step down and update term
	if msg.Term > n.currentTerm {
		n.stepDown(msg.Term)
	}

	// Ignore messages from outdated terms
	if msg.Term < n.currentTerm {
		log.Printf("Node %d ignoring outdated message from term %d", n.id, msg.Term)
		return
	}

	switch msg.Type {
	case types.VoteRequest:
		n.handleVoteRequest(msg)
	case types.VoteResponse:
		n.handleVoteResponse(msg)
	case types.AppendEntries, types.Heartbeat:
		n.handleAppendEntries(msg)
	case types.AppendEntriesResponse:
		n.handleAppendEntriesResponse(msg)
	case types.ClientRequest:
		n.handleClientRequest(msg)
	}
}

// handleVoteRequest processes vote requests from candidates
func (n *RaftNode) handleVoteRequest(msg types.Message) {
	voteGranted := false
	
	// Grant vote if:
	// 1. Haven't voted for anyone else in this term, AND
	// 2. Candidate's log is at least as up-to-date as ours
	if (n.votedFor == -1 || n.votedFor == msg.CandidateID) {
		lastLogIndex, lastLogTerm := n.getLastLogInfo()
		
		// Check if candidate's log is at least as up-to-date
		if msg.LastLogTerm > lastLogTerm || 
		   (msg.LastLogTerm == lastLogTerm && msg.LastLogIndex >= lastLogIndex) {
			voteGranted = true
			n.votedFor = msg.CandidateID
			n.resetElectionTimer()
			log.Printf("Node %d granted vote to Node %d for term %d", 
				n.id, msg.CandidateID, msg.Term)
		}
	}

	// Send vote response
	response := types.Message{
		Type:        types.VoteResponse,
		From:        n.id,
		To:          msg.From,
		Term:        n.currentTerm,
		VoteGranted: voteGranted,
	}
	
	n.sendMessage(response)
}

// handleVoteResponse processes vote responses when we're a candidate
func (n *RaftNode) handleVoteResponse(msg types.Message) {
    if n.state != types.Candidate {
        return
    }

    if msg.VoteGranted {
        log.Printf("Node %d received vote from Node %d", n.id, msg.From)
        n.votesReceived[msg.From] = true
        
        // Check if we have majority
        if n.hasMajority() {
            log.Printf("Node %d has majority votes, becoming leader", n.id)
            n.becomeLeader()
        }
    }
}

// handleAppendEntries processes append entries (including heartbeats) from leader
func (n *RaftNode) handleAppendEntries(msg types.Message) {
	success := false
	
	// Reset election timer since we heard from leader
	n.resetElectionTimer()
	n.currentLeader = msg.From
	n.lastHeartbeat = time.Now()
	
	// Convert to follower if we're candidate
	if n.state == types.Candidate {
		n.state = types.Follower
		if n.heartbeatTicker != nil {
            n.heartbeatTicker.Stop()
        }
	}
	
	// Check if our log contains an entry at prevLogIndex with prevLogTerm
    if msg.PrevLogIndex == -1 || 
       (msg.PrevLogIndex < len(n.log) && n.log[msg.PrevLogIndex].Term == msg.PrevLogTerm) {
        
        success = true
        
        // Remove conflicting entries and append new ones
        if len(msg.Entries) > 0 {
            // Truncate log if necessary
            if msg.PrevLogIndex+1 < len(n.log) {
                n.log = n.log[:msg.PrevLogIndex+1]
            }
            
            // Append new entries
            n.log = append(n.log, msg.Entries...)
            log.Printf("Node %d appended %d entries (log length: %d)", 
                n.id, len(msg.Entries), len(n.log))
        }
        
        // Update commit index
        if msg.LeaderCommit > n.commitIndex {
            oldCommitIndex := n.commitIndex
            n.commitIndex = min(msg.LeaderCommit, len(n.log)-1)
            if n.commitIndex > oldCommitIndex {
                log.Printf("Node %d updated commit index to %d", n.id, n.commitIndex)
                n.applyCommittedEntries()
            }
        }
    } else {
        log.Printf("Node %d rejected append entries: log inconsistency", n.id)
    }
	
	// Send response
	response := types.Message{
		Type:    types.AppendEntriesResponse,
		From:    n.id,
		To:      msg.From,
		Term:    n.currentTerm,
		Success: success,
		MatchIndex: len(n.log) - 1,
	}
	
	n.sendMessage(response)
}

// handleAppendEntriesResponse processes responses to our append entries
func (n *RaftNode) handleAppendEntriesResponse(msg types.Message) {
	if n.state != types.Leader {
		return
	}
	
	 // Find follower index
    followerIndex := -1
    for i, peer := range n.peers {
        if peer == msg.From {
            followerIndex = i
            break
        }
    }
    
    if followerIndex == -1 {
        return
    }
    
    if msg.Success {
        // Update nextIndex and matchIndex for follower
        n.nextIndex[followerIndex] = msg.MatchIndex + 1
        n.matchIndex[followerIndex] = msg.MatchIndex
        
        log.Printf("Leader %d: successful replication to node %d (matchIndex: %d)", 
            n.id, msg.From, msg.MatchIndex)
        
        // Try to update commit index
        n.updateCommitIndex()
    } else {
        // Decrement nextIndex and retry
        if n.nextIndex[followerIndex] > 0 {
            n.nextIndex[followerIndex]--
        }
        log.Printf("Leader %d: failed replication to node %d, retrying with nextIndex: %d", 
            n.id, msg.From, n.nextIndex[followerIndex])
        
        // Immediately retry with updated nextIndex
        n.replicateToFollower(msg.From)
    }
}


// handleClientRequest processes client requests (only leaders handle these)
func (n *RaftNode) handleClientRequest(msg types.Message) {
	if n.state != types.Leader {
        log.Printf("Node %d rejecting client request - not leader", n.id)
        return
    }

		 // Append to log
    logIndex := n.appendLogEntry(msg.Command)
    
    // Set up response channel for this command
    respCh := make(chan error, 1)
    n.pendingCommands[logIndex] = respCh
    
    // Replicate to all followers
    for _, peer := range n.peers {
        n.replicateToFollower(peer)
    }
    
    log.Printf("Leader %d processing client request at index %d: %v", 
        n.id, logIndex, msg.Command)
}

// handleElectionTimeout starts a new election
func (n *RaftNode) handleElectionTimeout() {
	n.mu.Lock()
	defer n.mu.Unlock()
	
	if n.state == types.Leader {
		return // Leaders don't start elections
	}
	
	n.startElection()
}

// handleHeartbeatTimeout sends heartbeats to followers (leaders only)
func (n *RaftNode) handleHeartbeatTimeout() {
	 n.mu.Lock()
    defer n.mu.Unlock()
    
    if n.state != types.Leader {
        return
    }
    
    // Send heartbeats/log entries to all followers
    for _, peer := range n.peers {
        n.replicateToFollower(peer)
    }
	
	n.sendHeartbeats()
}

// startElection begins a new election process
func (n *RaftNode) startElection() {
	n.state = types.Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.resetElectionTimer()
	
	log.Printf("Node %d starting election for term %d", n.id, n.currentTerm)
	
	// Send vote requests to all peers
	lastLogIndex, lastLogTerm := n.getLastLogInfo()
	
	for _, peer := range n.peers {
		voteRequest := types.Message{
			Type:         types.VoteRequest,
			From:         n.id,
			To:           peer,
			Term:         n.currentTerm,
			CandidateID:  n.id,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}
		n.sendMessage(voteRequest)
	}
}