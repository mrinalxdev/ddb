package partition

import (
	"crypto/sha1"
	"fmt"
	"sort"
	"sync"
)


type Node struct {
	ID       string
	Address  string 
	VNodes   []uint64 
}


type ConsistentHash struct {
	nodes       map[string]*Node
	ring        []ringEntry      
	numVNodes   int              
	replica     int              
	mu          sync.RWMutex     
}

type ringEntry struct {
	token uint64 // Hash value on the ring
	node  *Node  // Associated node
}


func NewConsistentHash(numVNodes, replica int) *ConsistentHash {
	return &ConsistentHash{
		nodes:     make(map[string]*Node),
		numVNodes: numVNodes,
		replica:   replica,
	}
}


func (ch *ConsistentHash) AddNode(id, address string) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	node := &Node{
		ID:      id,
		Address: address,
		VNodes:  make([]uint64, ch.numVNodes),
	}


	for i := 0; i < ch.numVNodes; i++ {
		token := ch.hash(fmt.Sprintf("%s-%d", id, i))
		node.VNodes[i] = token
		ch.ring = append(ch.ring, ringEntry{token: token, node: node})
	}

	ch.nodes[id] = node
	// Sort the ring by token
	sort.Slice(ch.ring, func(i, j int) bool {
		return ch.ring[i].token < ch.ring[j].token
	})
}

func (ch *ConsistentHash) RemoveNode(id string) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	_, exists := ch.nodes[id]
	if !exists {
		return
	}

	
	newRing := make([]ringEntry, 0, len(ch.ring)-ch.numVNodes)
	for _, entry := range ch.ring {
		if entry.node.ID != id {
			newRing = append(newRing, entry)
		}
	}
	ch.ring = newRing
	delete(ch.nodes, id)

	// Re-sort the ring
	sort.Slice(ch.ring, func(i, j int) bool {
		return ch.ring[i].token < ch.ring[j].token
	})
}


func (ch *ConsistentHash) GetNodesForKey(key string) []*Node {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	if len(ch.ring) == 0 {
		return nil
	}

	token := ch.hash(key)
	nodes := make([]*Node, 0, ch.replica)
	seen := make(map[string]bool)

	
	idx := ch.search(token)
	for i := 0; i < len(ch.ring) && len(nodes) < ch.replica; i++ {
		entry := ch.ring[(idx+i)%len(ch.ring)]
		if !seen[entry.node.ID] {
			nodes = append(nodes, entry.node)
			seen[entry.node.ID] = true
		}
	}

	return nodes
}


func (ch *ConsistentHash) hash(key string) uint64 {
	h := sha1.New()
	h.Write([]byte(key))
	bs := h.Sum(nil)
	// Use first 8 bytes for uint64
	var token uint64
	for i := 0; i < 8 && i < len(bs); i++ {
		token |= uint64(bs[i]) << (8 * i)
	}
	return token
}


func (ch *ConsistentHash) search(token uint64) int {
	return sort.Search(len(ch.ring), func(i int) bool {
		return ch.ring[i].token >= token
	}) % len(ch.ring)
}