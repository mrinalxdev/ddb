package partition

import (
	"fmt"
)

type Partitioner struct {
	ch *ConsistentHash
}

func NewPartitioner(numVNodes, replica int) *Partitioner {
	return &Partitioner{
		ch: NewConsistentHash(numVNodes, replica).
	}
}

// adding node to the partitioner
func (p *Partitioner) AddNode(id, address string){
	p.ch.AddNode(id, address)
}

func (p *Partitioner) RemoveNode(id string){
	p.ch.RemoveNode(id)
}

func (p*Partitioner) GetPartitionNodes(key string) ([]*Node, error){
	nodes := p.ch.GetNodesGorKey(key)
	if len(nodes) == 0 {
		return nil, fmt.Errorf("no nodes available for key : %s", key)
	}

	return nodes, nil
}
