/*
Core node implementation with message processing
*/
package node

import (
	"log"
	"math/rand"
	"sync"
	"time"
	"github.com/saiweb3dev/distributed_consensus_simulator/types"
)

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

	// Communication channels
	inbox  chan types.Message // Incoming messages
	outbox chan types.Message // Outgoing messages
	stopCh chan struct{}      // Signal to stop the node

	// Timing
	electionTimer   *time.Timer
	heartbeatTicker *time.Ticker
	lastHeartbeat   time.Time
}

// NewRaftNode creates a new Raft node
func NewRaftNode(id int, peers []int, config types.NodeConfig) *RaftNode {
	node := &RaftNode{
		id:            id,
		peers:         peers,
		config:        config,
		currentTerm:   0,
		votedFor:      -1,
		log:           make([]types.LogEntry, 0),
		commitIndex:   -1, // -1 indicates no entries committed yet
		lastApplied:   -1,
		state:         types.Follower,
		currentLeader: -1,
		inbox:         make(chan types.Message, 100),
		outbox:        make(chan types.Message, 100),
		stopCh:        make(chan struct{}),
		lastHeartbeat: time.Now(),
	}

	// Initialize leader state
	peerCount := len(peers) + 1 // +1 for self
	node.nextIndex = make([]int, peerCount)
	node.matchIndex = make([]int, peerCount)

	// Initialize timers
	node.electionTimer = time.NewTimer(node.randomElectionTimeout())
	node.heartbeatTicker = time.NewTicker(config.HeartbeatInterval)
	node.heartbeatTicker.Stop() // Only leaders send heartbeats

	return node
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

		case <-n.heartbeatTicker.C:
			n.handleHeartbeatTimeout()

		case <-n.stopCh:
			log.Printf("Node %d stopping", n.id)
			n.electionTimer.Stop()
			n.heartbeatTicker.Stop()
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
		
		// Count votes (including our own vote)
		votes := 1
		for _, peer := range n.peers {
			// In a real implementation, we'd track votes received
			// For now, we'll simulate becoming leader after receiving first vote
			log.Printf("Node %d checking vote from Node %d", n.id, peer)
		}
		
		// If we have majority, become leader
		majority := (len(n.peers) + 1) / 2 + 1
		if votes >= majority {
			n.becomeLeader()
		}
	}
}

// handleAppendEntries processes append entries (including heartbeats) from leader
func (n *RaftNode) handleAppendEntries(msg types.Message) {
	success := true
	
	// Reset election timer since we heard from leader
	n.resetElectionTimer()
	n.currentLeader = msg.From
	
	// Convert to follower if we're candidate
	if n.state == types.Candidate {
		n.state = types.Follower
		n.heartbeatTicker.Stop()
	}
	
	log.Printf("Node %d received %s from leader %d", n.id, msg.Type, msg.From)
	
	// Send response
	response := types.Message{
		Type:    types.AppendEntriesResponse,
		From:    n.id,
		To:      msg.From,
		Term:    n.currentTerm,
		Success: success,
	}
	
	n.sendMessage(response)
}

// handleAppendEntriesResponse processes responses to our append entries
func (n *RaftNode) handleAppendEntriesResponse(msg types.Message) {
	if n.state != types.Leader {
		return
	}
	
	log.Printf("Leader %d received AppendEntriesResponse from Node %d (success: %t)", 
		n.id, msg.From, msg.Success)
}


// handleClientRequest processes client requests (only leaders handle these)
func (n *RaftNode) handleClientRequest(msg types.Message) {
	if n.state != types.Leader {
		// Redirect to leader if known
		if n.currentLeader != -1 {
			log.Printf("Node %d redirecting client request to leader %d", n.id, n.currentLeader)
		} else {
			log.Printf("Node %d rejecting client request - no known leader", n.id)
		}
		return
	}
	
	log.Printf("Leader %d received client request: %v", n.id, msg.Command)
	// In Module 3, we'll implement log replication here
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

// becomeLeader transitions this node to leader state
func (n *RaftNode) becomeLeader() {
	log.Printf("Node %d became leader for term %d", n.id, n.currentTerm)
	
	n.state = types.Leader
	n.currentLeader = n.id
	
	// Initialize leader state
	nextIndex := len(n.log)
	for i := range n.nextIndex {
		n.nextIndex[i] = nextIndex
		n.matchIndex[i] = -1
	}
	
	// Start sending heartbeats
	n.electionTimer.Stop()
	n.heartbeatTicker.Reset(n.config.HeartbeatInterval)
	
	// Send immediate heartbeat
	n.sendHeartbeats()
}

// stepDown converts to follower state
func (n *RaftNode) stepDown(newTerm int) {
	log.Printf("Node %d stepping down from %s to Follower (term %d -> %d)", 
		n.id, n.state, n.currentTerm, newTerm)
	
	n.currentTerm = newTerm
	n.votedFor = -1
	n.state = types.Follower
	n.currentLeader = -1
	
	n.heartbeatTicker.Stop()
	n.resetElectionTimer()
}


// sendHeartbeats sends heartbeat messages to all followers
func (n *RaftNode) sendHeartbeats() {
	for _, peer := range n.peers {
		heartbeat := types.Message{
			Type: types.Heartbeat,
			From: n.id,
			To:   peer,
			Term: n.currentTerm,
		}
		n.sendMessage(heartbeat)
	}
}

// Helper methods
func (n *RaftNode) sendMessage(msg types.Message) {
	select {
	case n.outbox <- msg:
	default:
		log.Printf("Node %d outbox full, dropping message", n.id)
	}
}

func (n *RaftNode) getLastLogInfo() (int, int) {
	if len(n.log) == 0 {
		return -1, 0
	}
	lastEntry := n.log[len(n.log)-1]
	return lastEntry.Index, lastEntry.Term
}

func (n *RaftNode) randomElectionTimeout() time.Duration {
	min := n.config.ElectionTimeoutMin
	max := n.config.ElectionTimeoutMax
	return min + time.Duration(rand.Int63n(int64(max-min)))
}

func (n *RaftNode) resetElectionTimer() {
	n.electionTimer.Stop()
	n.electionTimer.Reset(n.randomElectionTimeout())
}

// Public API methods
func (n *RaftNode) GetState() (types.NodeState, int, int) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.state, n.currentTerm, n.currentLeader
}

func (n *RaftNode) SendMessage(msg types.Message) {
	select {
	case n.inbox <- msg:
	default:
		log.Printf("Node %d inbox full, dropping message", n.id)
	}
}

func (n *RaftNode) GetOutboxMessage() (types.Message, bool) {
	select {
	case msg := <-n.outbox:
		return msg, true
	default:
		return types.Message{}, false
	}
}

func (n *RaftNode) Stop() {
	close(n.stopCh)
}