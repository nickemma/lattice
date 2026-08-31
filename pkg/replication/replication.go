// Package replication is the deliberately unsafe Phase 2 store. It makes
// split-brain and stale-read behavior observable before Raft is introduced.
package replication

import (
	"errors"
	"fmt"
	"sync"
)

var ErrUnknownNode = errors.New("replication: unknown node")

type Node struct {
	ID   int
	Data map[string]string
}

type Cluster struct {
	mu    sync.RWMutex
	nodes map[int]*Node
}

func NewCluster(ids ...int) *Cluster {
	c := &Cluster{nodes: make(map[int]*Node)}
	for _, id := range ids {
		c.nodes[id] = &Node{ID: id, Data: make(map[string]string)}
	}
	return c
}

// Write writes only to the selected replica. There is intentionally no
// leader, quorum, or conflict resolution in this phase.
func (c *Cluster) Write(nodeID int, key, value string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	node, ok := c.nodes[nodeID]
	if !ok {
		return fmt.Errorf("%w: %d", ErrUnknownNode, nodeID)
	}
	node.Data[key] = value
	return nil
}

func (c *Cluster) Read(nodeID int, key string) (string, bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	node, ok := c.nodes[nodeID]
	if !ok {
		return "", false, fmt.Errorf("%w: %d", ErrUnknownNode, nodeID)
	}
	value, found := node.Data[key]
	return value, found, nil
}

func (c *Cluster) Sync(source, destination int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	src, ok := c.nodes[source]
	if !ok {
		return fmt.Errorf("%w: %d", ErrUnknownNode, source)
	}
	dst, ok := c.nodes[destination]
	if !ok {
		return fmt.Errorf("%w: %d", ErrUnknownNode, destination)
	}
	dst.Data = make(map[string]string, len(src.Data))
	for key, value := range src.Data {
		dst.Data[key] = value
	}
	return nil
}

func (c *Cluster) Snapshot() []Node {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make([]Node, 0, len(c.nodes))
	for _, node := range c.nodes {
		copyNode := Node{ID: node.ID, Data: make(map[string]string, len(node.Data))}
		for key, value := range node.Data {
			copyNode.Data[key] = value
		}
		result = append(result, copyNode)
	}
	return result
}
