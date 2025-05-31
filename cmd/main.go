package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// States of a Raft Node
type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

func (s NodeState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// LogEntry represents a single entry in the Raft Log
type LogEntry struct {
	Term int     // Term when entry was received by leader
	Index int    // Position in the log
	Command interface{} // State machine command
}

// Message types for inter-node communication
type MessageType int

const (
	VoteRequest MessageType = iota
	VoteResponse
	AppendEntries
	AppendEntriesResponse 
	Heartbeat
)

func (m MessageType) String() string {
	switch m {
	case VoteRequest:
		return "VoteRequest"
	case VoteResponse:
		return "VoteResponse"
	case AppendEntries:
		return "AppendEntries"
	case AppendEntriesResponse:
		return "AppendEntriesResponse"
	case Heartbeat:
		return "Heartbeat"
	default:
		return "Unknown"
	}
}

// Message represents communication between nodes
type Message struct {
	Type         MessageType
	From         int // Sender node ID
	To           int // Receiver node ID
	Term         int // Sender's current term
	CandidateID  int // For vote requests
	LastLogIndex int // For vote requests and append entries
	LastLogTerm  int // For vote requests and append entries
	Entries      []LogEntry // For append entries
	VoteGranted  bool // For vote responses
	Success      bool // For append entries responses
}

// RaftNode represents a single node in the Raft cluster
type RaftNode struct {
	// Persistent state (would be stored on disk in real implementation)
	id          int        // Node identifier
	currentTerm int        // Latest term server has seen
	votedFor    int        // CandidateId that received vote in current term (-1 if none)
	log         []LogEntry // Log entries

	// Volatile state
	commitIndex int // Index of highest log entry known to be committed
	lastApplied int // Index of highest log entry applied to state machine

	// Leader state (reinitialized after election)
	nextIndex  []int // For each server, index of next log entry to send
	matchIndex []int // For each server, index of highest log entry known to be replicated

	// Node state management
	state       NodeState
	mu          sync.RWMutex // Protects shared state
	peers       []int        // IDs of other nodes in cluster
	
	// Communication channels
	inbox       chan Message     // Incoming messages
	outbox      chan Message     // Outgoing messages
	stopCh      chan struct{}    // Signal to stop the node
	
	// Timing
	electionTimeout  time.Duration
	heartbeatTimeout time.Duration
	lastHeartbeat    time.Time
}

// NewRaftNode creates a new Raft node
func NewRaftNode(id int, peers []int) *RaftNode {
	node := &RaftNode{
		id:               id,
		currentTerm:      0,
		votedFor:         -1,
		log:              make([]LogEntry, 0),
		commitIndex:      0,
		lastApplied:      0,
		state:           Follower,
		peers:           peers,
		inbox:           make(chan Message, 100),
		outbox:          make(chan Message, 100),
		stopCh:          make(chan struct{}),
		electionTimeout: randomElectionTimeout(),
		heartbeatTimeout: 50 * time.Millisecond,
		lastHeartbeat:   time.Now(),
	}
	// Initialize leader state
	peerCount := len(peers) + 1 // +1 for self
	node.nextIndex = make([]int, peerCount)
	node.matchIndex = make([]int, peerCount)
	
	return node
}

// randomElectionTimeout generates a random timeout between 150-300ms
// This randomization helps prevent split votes
func randomElectionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

// GetState returns the current state of the node (thread-safe)
func (n *RaftNode) GetState() (NodeState, int) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.state, n.currentTerm
}

// GetLogInfo returns information about the node's log (thread-safe)
func (n *RaftNode) GetLogInfo() (int, int) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	
	lastIndex := len(n.log) - 1
	lastTerm := 0
	if lastIndex >= 0 {
		lastTerm = n.log[lastIndex].Term
	}
	return lastIndex, lastTerm
}

// Stop gracefully stops the node
func (n *RaftNode) Stop() {
	close(n.stopCh)
}

// Demo function to show the basic structure
func main() {
	// Create a small cluster of 3 nodes
	peers := []int{1, 2} // Node 0's peers are nodes 1 and 2
	
	node := NewRaftNode(0, peers)
	state, term := node.GetState()
	
	fmt.Printf("Created Node %d\n", node.id)
	fmt.Printf("Initial State: %s\n", state)
	fmt.Printf("Initial Term: %d\n", term)
	fmt.Printf("Peers: %v\n", node.peers)
	fmt.Printf("Election Timeout: %v\n", node.electionTimeout)
	
	// Create a sample log entry
	entry := LogEntry{
		Term:    1,
		Index:   0,
		Command: "SET x=10",
	}
	
	fmt.Printf("Sample Log Entry: Term=%d, Index=%d, Command=%v\n", 
		entry.Term, entry.Index, entry.Command)
	
	// Create a sample message
	msg := Message{
		Type:        VoteRequest,
		From:        0,
		To:          1,
		Term:        1,
		CandidateID: 0,
	}
	
	fmt.Printf("Sample Message: %s from Node %d to Node %d (Term: %d)\n",
		msg.Type, msg.From, msg.To, msg.Term)
	
	node.Stop()
}