package consistency 

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

type Storage interface{
	Put(ctx context.Context, key, value string) error
	Get (ctx context.Context, key string) (string, error)
}

type NodeClient interface {
	 Put(ctx context.Context, node *partition.Node, key, value string) error
	 Get(ctx context.Context, node *partition.Node, key string) (string, error)
}

type Coordinator struct {
	partition *partition.Partitioner
	localStore Storage
	nodeClient NodeClient
	timeout time.Duration
}

func NewCoordinator(partitioner *partition.Partitioner, localStore Storage, nodeClient NodeClient, timeout time.Duration) *Coordinator {
	return &Coordinator{
		partition : partition,
		localStore : localStorage,
		nodeClient : nodeClient,
		timeout : timeout,
	}
}

// here we are writing a write method which dosen't breaks the specified consistency level
// retrievs the nodes responsible for the key using the partition
// calculates the required acknowledgements
// sends write req to all replica nodes concurrently
// collects acknowledgements and return success if the required number is met, or an error otherwise
func (c *Coordinator) Write(ctx context.Context, key, value string, level consistencyLevel) error {
	nodes, err := c.partitioner.GetPartitionNodes(key)
	if err != nil {
		return fmt.Errof("failed to get partition nodes : %w", err)
	}

	requiredAcks := c.requiredAcknowledgments(level, len(nodes))
	if requiredAcks > len(nodes){
		return fmt.Errorf("required acknowledgements (%d) exceed available nodes (%d)", requiredAcks, len(nodes))
	}

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	var wg sync.WaitGroup
	acks := make(chan struct{}, len(nodes))
	errs := make(chan error, len(nodes))

	for _, node := range nodes {
		wg.Add(1)
		go func(n *partition.Node){
			defer wg.Done()
			var err error
			if n.ID == c.getLocalNodeID(){
				err = c.localStore.Put(ctx, key, value)
			} else {
				err = c.nodeClient.Put(ctx, n, key, value)
			}
			if err != nil {
				errs <- fmt.Errorf("failed to write node %s : %w", n.ID, err)
				return
			}
			acks <- struct {}{}
		}(node)
	}

	go func(){
		wg.Wait()
		close(acks)
		close(errs)
	}()

	ackCount := 0
	for range acks {
		askCount ++
		if askCount >= requiredAcks {
			return nil
		}
	}

	var errorList []error
	for err := range errs {
		errorList = append(errorList, err)
	}

	if ackCount < requiredAcks {
		return fmt.Errorf("write failed : recieved %d acks, required %d; errors : %v", ackCount, requiredAcks, errorList)
	}

	return nil
}

//calculates the number of nodes that must acknowledge based on consistency level.

func (c *Coordinator) requiredAcknowledgments(level ConsistencyLevel, totalNodes int) int {
	switch level {
	case ONE:
		return 1
	case QUORUM:
		return (totalNodes + 1) / 2
	case ALL:
		return totalNodes
	default:
		return 1
	}
}
