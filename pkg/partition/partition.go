package partition

import (
	"fmt"
)

// Partitioner manages data partitioning across nodes.
type Partitioner struct {
	ch *ConsistentHash
}

// NewPartitioner creates a new partitioner with the given configuration.
func NewPartitioner(numVNodes, replica int) *Partitioner {
	return &Partitioner{
		ch: NewConsistentHash(numVNodes, replica),
	}
}

// AddNode adds a node to the partitioner.
func (p *Partitioner) AddNode(id, address string) {
	p.ch.AddNode(id, address)
}

// RemoveNode removes a node from the partitioner.
func (p *Partitioner) RemoveNode(id string) {
	p.ch.RemoveNode(id)
}

// GetPartitionNodes returns the nodes responsible for a key.
func (p *Partitioner) GetPartitionNodes(key string) ([]*Node, error) {
	nodes := p.ch.GetNodesForKey(key)
	if len(nodes) == 0 {
		return nil, fmt.Errorf("no nodes available for key: %s", key)
	}
	return nodes, nil
}