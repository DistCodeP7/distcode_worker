package centralized_mutex

import (
	"sync"
	"time"
)

// Node represents a participant with an inbox and a handler.
type Node struct {
	ID      string
	Inbox   chan Message
	Handler MessageHandler
	wg      sync.WaitGroup
}

func (n *Node) StartLoop(net *Network) {
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		for msg := range n.Inbox {
			outs := n.Handler.OnMessage(msg)
			for _, out := range outs {
				net.SendAsync(out)
			}
		}
	}()
}

func (n *Node) Stop() {
	close(n.Inbox)
	n.wg.Wait()
}

// Network is a simple in-memory message router.
type Network struct {
	nodes          map[string]*Node
	mu             sync.RWMutex
	sendBufferSize int
}

func NewNetwork(sendBufferSize int) *Network {
	return &Network{
		nodes:          make(map[string]*Node),
		sendBufferSize: sendBufferSize,
	}
}

func (net *Network) Register(node *Node) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.nodes[node.ID] = node
}

func (net *Network) Unregister(id string) {
	net.mu.Lock()
	defer net.mu.Unlock()
	delete(net.nodes, id)
}

func (net *Network) SendAsync(msg Message) {
	net.mu.RLock()
	recipient, ok := net.nodes[msg.To]
	net.mu.RUnlock()
	if !ok {
		return
	}

	go func() {
		select {
		case recipient.Inbox <- msg:
		case <-time.After(500 * time.Millisecond):
		}
	}()
}
