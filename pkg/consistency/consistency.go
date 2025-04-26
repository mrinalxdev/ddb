package consistency

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"ddb/pkg/partition"
)


type Storage interface {
	Put(ctx context.Context, key, value string) error
	Get(ctx context.Context, key string) (string, error)
}

type NodeClient interface {
	Put(ctx context.Context, node *partition.Node, key, value string) error
	Get(ctx context.Context, node *partition.Node, key string) (string, error)
}

// Coordinator manages consistency for read/write operations.
type Coordinator struct {
	partitioner *partition.Partitioner
	localStore  Storage
	nodeClient  NodeClient
	timeout     time.Duration
}

func NewCoordinator(partitioner *partition.Partitioner, localStore Storage, nodeClient NodeClient, timeout time.Duration) *Coordinator {
	return &Coordinator{
		partitioner: partitioner,
		localStore:  localStore,
		nodeClient:  nodeClient,
		timeout:     timeout,
	}
}


func (c *Coordinator) Write(ctx context.Context, key, value string, level ConsistencyLevel) error {
	nodes, err := c.partitioner.GetPartitionNodes(key)
	if err != nil {
		return fmt.Errorf("failed to get partition nodes: %w", err)
	}

	requiredAcks := c.requiredAcknowledgments(level, len(nodes))
	if requiredAcks > len(nodes) {
		return fmt.Errorf("required acknowledgments (%d) exceed available nodes (%d)", requiredAcks, len(nodes))
	}

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	var wg sync.WaitGroup
	acks := make(chan struct{}, len(nodes))
	errs := make(chan error, len(nodes))

	for _, node := range nodes {
		wg.Add(1)
		go func(n *partition.Node) {
			defer wg.Done()
			var err error
			if n.ID == c.GetLocalNodeId() {
				err = c.localStore.Put(ctx, key, value)
			} else {
				err = c.nodeClient.Put(ctx, n, key, value)
			}
			if err != nil {
				errs <- fmt.Errorf("failed to write to node %s: %w", n.ID, err)
				return
			}
			acks <- struct{}{}
		}(node)
	}

	
	go func() {
		wg.Wait()
		close(acks)
		close(errs)
	}()

	ackCount := 0
	for range acks {
		ackCount++
		if ackCount >= requiredAcks {
			return nil
		}
	}

	
	var errorList []error
	for err := range errs {
		errorList = append(errorList, err)
	}

	if ackCount < requiredAcks {
		return fmt.Errorf("write failed: received %d acks, required %d; errors: %v", ackCount, requiredAcks, errorList)
	}

	return nil
}


func (c *Coordinator) Read(ctx context.Context, key string, level ConsistencyLevel) (string, error) {
	nodes, err := c.partitioner.GetPartitionNodes(key)
	if err != nil {
		return "", fmt.Errorf("failed to get partition nodes: %w", err)
	}

	requiredAcks := c.requiredAcknowledgments(level, len(nodes))
	if requiredAcks > len(nodes) {
		return "", fmt.Errorf("required acknowledgments (%d) exceed available nodes (%d)", requiredAcks, len(nodes))
	}

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	var wg sync.WaitGroup
	results := make(chan string, len(nodes))
	errs := make(chan error, len(nodes))

	for _, node := range nodes {
		wg.Add(1)
		go func(n *partition.Node) {
			defer wg.Done()
			var value string
			var err error
			if n.ID == c.GetLocalNodeId() {
				value, err = c.localStore.Get(ctx, key)
			} else {
				value, err = c.nodeClient.Get(ctx, n, key)
			}
			if err != nil {
				errs <- fmt.Errorf("failed to read from node %s: %w", n.ID, err)
				return
			}
			results <- value
		}(node)
	}

	
	go func() {
		wg.Wait()
		close(results)
		close(errs)
	}()

	ackCount := 0
	var latestValue string
	for value := range results {
		ackCount++
		latestValue = value
		if ackCount >= requiredAcks {
			return latestValue, nil
		}
	}


	var errorList []error
	for err := range errs {
		errorList = append(errorList, err)
	}

	if ackCount < requiredAcks {
		return "", fmt.Errorf("read failed: received %d acks, required %d; errors: %v", ackCount, requiredAcks, errorList)
	}

	return "", errors.New("no value returned")
}


func (c *Coordinator) requiredAcknowledgments(level ConsistencyLevel, totalNodes int) int {
	switch level {
	case ONE:
		return 1
	case QUORUM:
		return (totalNodes + 1) / 2
	case ALL:
		return totalNodes
	default:
		return 1 // Default to ONE
	}
}


func (c *Coordinator) GetLocalNodeId() string {
	// Placeholder: In a real system, this would be configured or discovered.
	return "local-node"
}