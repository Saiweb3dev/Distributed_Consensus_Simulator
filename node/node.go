/*
Core node implementation with message processing
*/
package node

import (
	"log"
	"sync"
	"time"
	"errors"
	"fmt"
	"github.com/saiweb3dev/distributed_consensus_simulator/types"
)

// MessageRouter interface for sending messages
type MessageRouter interface {
	RouteMessage(msg types.Message)
}

// StateMachine handles state transitions
type StateMachine struct {
    data map[string]interface{}
}

// NewStateMachine creates a new state machine
func NewStateMachine() *StateMachine {
    return &StateMachine{
        data: make(map[string]interface{}),
    }
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
   stateMachine    *StateMachine // Simple key-value state machine
    pendingCommands map[int]chan error  // Track pending client commands
}

// Apply applies a command to the state machine
func (sm *StateMachine) Apply(command interface{}) error {
    cmd, ok := command.(map[string]interface{})
    if !ok {
        return errors.New("invalid command format")
    }

    cmdType, exists := cmd["type"]
    if !exists {
        cmdType = cmd["Type"] // Handle struct field
    }

    switch cmdType {
    case "SET":
        return sm.handleSet(cmd)
    case "CREATE_ACCOUNT":
        return sm.handleCreateAccount(cmd)
    case "TRANSFER":
        return sm.handleTransfer(cmd)
    default:
        return fmt.Errorf("unknown command type: %v", cmdType)
    }
}

// handleSet handles SET operations
func (sm *StateMachine) handleSet(cmd map[string]interface{}) error {
    key, keyOk := getStringField(cmd, "key", "Key")
    value, valueOk := getField(cmd, "value", "Value")
    
    if !keyOk || !valueOk {
        return errors.New("SET: missing key or value")
    }

    sm.data[key] = value
    return nil
}

// handleCreateAccount handles account creation
func (sm *StateMachine) handleCreateAccount(cmd map[string]interface{}) error {
    accountID, ok := getStringField(cmd, "key", "Key")
    if !ok {
        return errors.New("CREATE_ACCOUNT: missing account ID")
    }

    amount, ok := getFloatField(cmd, "amount", "Amount")
    if !ok {
        return errors.New("CREATE_ACCOUNT: missing initial amount")
    }

    balanceKey := "balance_" + accountID
    if _, exists := sm.data[balanceKey]; exists {
        return fmt.Errorf("account %s already exists", accountID)
    }

    sm.data[balanceKey] = amount
    sm.data["account_"+accountID] = map[string]interface{}{
        "created_at": fmt.Sprintf("%d", getCurrentTimestamp()),
        "status":     "active",
    }

    return nil
}

// handleTransfer handles money transfers
func (sm *StateMachine) handleTransfer(cmd map[string]interface{}) error {
    from, fromOk := getStringField(cmd, "from", "From")
    to, toOk := getStringField(cmd, "to", "To")
    amount, amountOk := getFloatField(cmd, "amount", "Amount")

    if !fromOk || !toOk || !amountOk {
        return errors.New("TRANSFER: missing required fields")
    }

    if amount <= 0 {
        return errors.New("TRANSFER: amount must be positive")
    }

    fromBalanceKey := "balance_" + from
    toBalanceKey := "balance_" + to

    // Check if accounts exist
    fromBalance, fromExists := sm.data[fromBalanceKey]
    toBalance, toExists := sm.data[toBalanceKey]

    if !fromExists {
        return fmt.Errorf("account %s does not exist", from)
    }
    if !toExists {
        return fmt.Errorf("account %s does not exist", to)
    }

    // Convert to float64
    fromBal, ok := fromBalance.(float64)
    if !ok {
        return fmt.Errorf("invalid balance format for account %s", from)
    }

    toBal, ok := toBalance.(float64)
    if !ok {
        return fmt.Errorf("invalid balance format for account %s", to)
    }

    // Check sufficient funds
    if fromBal < amount {
        return fmt.Errorf("insufficient funds: %s has $%.2f, needs $%.2f", from, fromBal, amount)
    }

    // Perform transfer
    sm.data[fromBalanceKey] = fromBal - amount
    sm.data[toBalanceKey] = toBal + amount

    // Log transaction
    txnKey := fmt.Sprintf("txn_%d", getCurrentTimestamp())
    sm.data[txnKey] = map[string]interface{}{
        "from":   from,
        "to":     to,
        "amount": amount,
        "type":   "transfer",
    }

    return nil
}

// Get retrieves a value from the state machine
func (sm *StateMachine) Get(key string) (interface{}, bool) {
    value, exists := sm.data[key]
    return value, exists
}

// GetAll returns a copy of all data
func (sm *StateMachine) GetAll() map[string]interface{} {
    result := make(map[string]interface{})
    for k, v := range sm.data {
        result[k] = v
    }
    return result
}

// Helper functions
func getField(cmd map[string]interface{}, keys ...string) (interface{}, bool) {
    for _, key := range keys {
        if value, exists := cmd[key]; exists {
            return value, true
        }
    }
    return nil, false
}

func getStringField(cmd map[string]interface{}, keys ...string) (string, bool) {
    value, exists := getField(cmd, keys...)
    if !exists {
        return "", false
    }
    str, ok := value.(string)
    return str, ok
}

func getFloatField(cmd map[string]interface{}, keys ...string) (float64, bool) {
    value, exists := getField(cmd, keys...)
    if !exists {
        return 0, false
    }
    if f, ok := value.(float64); ok {
        return f, true
    }
    if i, ok := value.(int); ok {
        return float64(i), true
    }
    return 0, false
}

func getCurrentTimestamp() int64 {
    return 1000000 + int64(len("dummy")) // Simple timestamp simulation
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
				stateMachine:   NewStateMachine(),
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
	
	logImportant("🗳️  Node %d starting election for term %d", n.id, n.currentTerm)

	// Reset vote tracking
    n.votesReceived = make(map[int]bool)
    n.votesReceived[n.id] = true // Vote for ourselves
	
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