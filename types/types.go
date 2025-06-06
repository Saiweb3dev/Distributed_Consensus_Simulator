package types

import "time"

// NodeState represents the three possible states of a Raft node
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

// LogEntry represents a single entry in the Raft log
type LogEntry struct {
	Term    int         `json:"term"`
	Index   int         `json:"index"`
	Command interface{} `json:"command"`
}

// Message types for inter-node communication
type MessageType int

const (
	VoteRequest MessageType = iota
	VoteResponse
	AppendEntries
	AppendEntriesResponse
	Heartbeat
	ClientRequest
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
	case ClientRequest:
		return "ClientRequest"
	default:
		return "Unknown"
	}
}

// Message represents communication between nodes
type Message struct {
	Type         MessageType `json:"type"`
	From         int         `json:"from"`
	To           int         `json:"to"`
	Term         int         `json:"term"`
	CandidateID  int         `json:"candidate_id,omitempty"`
	LastLogIndex int         `json:"last_log_index,omitempty"`
	LastLogTerm  int         `json:"last_log_term,omitempty"`
	Entries      []LogEntry  `json:"entries,omitempty"`
	VoteGranted  bool        `json:"vote_granted,omitempty"`
	Success      bool        `json:"success,omitempty"`
	LeaderCommit int         `json:"leader_commit,omitempty"`
	Command      interface{} `json:"command,omitempty"`
   
	// Add fields for log replication
    PrevLogIndex int `json:"prev_log_index,omitempty"`
    PrevLogTerm  int `json:"prev_log_term,omitempty"`
    MatchIndex   int `json:"match_index,omitempty"`   // For tracking replication progress
		
	// Network simulation fields
	Timestamp time.Time `json:"timestamp"`
	Delay     time.Duration `json:"delay,omitempty"`
}

// NetworkConfig holds network simulation parameters
type NetworkConfig struct {
	BaseLatency    time.Duration // Base network latency
	LatencyJitter  time.Duration // Random latency variation
	PacketLoss     float64       // Probability of packet loss (0.0 to 1.0)
	PartitionNodes []int         // Nodes that are partitioned
	Enabled        bool          // Whether network simulation is enabled
}

// NodeConfig holds configuration for a Raft node
type NodeConfig struct {
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
	RequestTimeout     time.Duration
}

// DefaultNodeConfig returns sensible defaults
func DefaultNodeConfig() NodeConfig {
	return NodeConfig{
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
		RequestTimeout:     100 * time.Millisecond,
	}
}
// DefaultNetworkConfig returns default network simulation parameters
func DefaultNetworkConfig() NetworkConfig {
    return NetworkConfig{
        BaseLatency:    10 * time.Millisecond,
        LatencyJitter:  5 * time.Millisecond,
        PacketLoss:     0.0,
        PartitionNodes: []int{},
        Enabled:        true,
    }
}

// NodeInfo represents public information about a node
type NodeInfo struct {
	ID            int       `json:"id"`
	State         NodeState `json:"state"`
	Term          int       `json:"term"`
	Leader        int       `json:"leader"`
	LogLength     int       `json:"log_length"`
	CommitIndex   int       `json:"commit_index"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
}