package client

import (
    "errors"
    "fmt"
    "math/rand"
    "time"
    "github.com/saiweb3dev/distributed_consensus_simulator/node"
)

// Client represents a client that can interact with the Raft cluster
type Client struct {
    id      string
    nodes   []*node.RaftNode
    timeout time.Duration
}

// ClientRequest represents different types of operations
type ClientRequest struct {
    Type      string      `json:"type"`
    Key       string      `json:"key,omitempty"`
    Value     interface{} `json:"value,omitempty"`
    Amount    float64     `json:"amount,omitempty"`
    From      string      `json:"from,omitempty"`
    To        string      `json:"to,omitempty"`
    RequestID string      `json:"request_id"`
}

// NewClient creates a new client
func NewClient(id string, nodes []*node.RaftNode) *Client {
    return &Client{
        id:      id,
        nodes:   nodes,
        timeout: 5 * time.Second,
    }
}

// Set stores a key-value pair
func (c *Client) Set(key string, value interface{}) error {
    req := ClientRequest{
        Type:      "SET",
        Key:       key,
        Value:     value,
        RequestID: c.generateRequestID(),
    }
    return c.sendRequest(req)
}

// Get retrieves a value (read from any node's state machine)
func (c *Client) Get(key string) (interface{}, error) {
    // For reads, we can query any node's committed state
    for _, node := range c.nodes {
        stateMachine := node.GetStateMachine()
        if value, exists := stateMachine[key]; exists {
            return value, nil
        }
    }
    return nil, errors.New("key not found")
}

// Transfer simulates a money transfer (atomic operation)
func (c *Client) Transfer(from, to string, amount float64) error {
    req := ClientRequest{
        Type:      "TRANSFER",
        From:      from,
        To:        to,
        Amount:    amount,
        RequestID: c.generateRequestID(),
    }
    return c.sendRequest(req)
}

// CreateAccount creates a new account with initial balance
func (c *Client) CreateAccount(accountID string, initialBalance float64) error {
    req := ClientRequest{
        Type:      "CREATE_ACCOUNT",
        Key:       accountID,
        Amount:    initialBalance,
        RequestID: c.generateRequestID(),
    }
    return c.sendRequest(req)
}

// GetBalance gets account balance
func (c *Client) GetBalance(accountID string) (float64, error) {
    value, err := c.Get("balance_" + accountID)
    if err != nil {
        return 0, err
    }
    if balance, ok := value.(float64); ok {
        return balance, nil
    }
    return 0, errors.New("invalid balance format")
}

// sendRequest sends a request to the current leader
func (c *Client) sendRequest(req ClientRequest) error {
    leader := c.findLeader()
    if leader == nil {
        return errors.New("no leader available")
    }

    err := leader.SubmitCommand(req)
    if err != nil {
        return fmt.Errorf("failed to submit command: %v", err)
    }

    // Wait a bit for consensus
    time.Sleep(100 * time.Millisecond)
    return nil
}

// findLeader finds the current leader node
func (c *Client) findLeader() *node.RaftNode {
    for _, node := range c.nodes {
        if node.IsLeader() {
            return node
        }
    }
    return nil
}

// generateRequestID generates a unique request ID
func (c *Client) generateRequestID() string {
    return fmt.Sprintf("%s-%d-%d", c.id, time.Now().UnixNano(), rand.Intn(1000))
}

// SimulateBankingWorkload simulates realistic banking operations
func (c *Client) SimulateBankingWorkload(duration time.Duration) {
    accounts := []string{"alice", "bob", "charlie", "david"}
    
    // Create accounts
    for _, account := range accounts {
        c.CreateAccount(account, 1000.0)
        time.Sleep(50 * time.Millisecond)
    }

    endTime := time.Now().Add(duration)
    operationCount := 0

    for time.Now().Before(endTime) {
        operation := rand.Intn(3)
        
        switch operation {
        case 0: // Transfer money
            from := accounts[rand.Intn(len(accounts))]
            to := accounts[rand.Intn(len(accounts))]
            if from != to {
                amount := float64(rand.Intn(100)) + 1
                err := c.Transfer(from, to, amount)
                if err == nil {
                    operationCount++
                    fmt.Printf("✓ Transfer: %s -> %s ($%.2f)\n", from, to, amount)
                }
            }
        case 1: // Check balance
            account := accounts[rand.Intn(len(accounts))]
            balance, err := c.GetBalance(account)
            if err == nil {
                fmt.Printf("📊 Balance %s: $%.2f\n", account, balance)
            }
        case 2: // Set arbitrary data
            key := fmt.Sprintf("data_%d", rand.Intn(10))
            value := fmt.Sprintf("value_%d", time.Now().Unix())
            err := c.Set(key, value)
            if err == nil {
                operationCount++
                fmt.Printf("💾 Set %s = %s\n", key, value)
            }
        }
        
        time.Sleep(time.Duration(rand.Intn(300)+100) * time.Millisecond)
    }

    fmt.Printf("\n🏁 Completed %d operations in %v\n", operationCount, duration)
}