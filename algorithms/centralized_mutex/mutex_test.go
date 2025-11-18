package centralized_mutex

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestCentralizedMutex(t *testing.T) {
	net := NewNetwork(10)
	nodes := []string{"A", "B", "C", "D", "E"}

	server := NewMutexServer("A", true)
	clients := make(map[string]*MutexClient)

	for _, id := range nodes {
		hasToken := id == "A"
		clients[id] = NewMutexClient(id, hasToken)
	}

	for _, id := range nodes {
		var handler MessageHandler
		if id == "A" {
			handler = &CombinedHandler{
				Server: server,
				Client: clients[id],
			}
		} else {
			handler = clients[id]
		}

		node := &Node{
			ID:      id,
			Inbox:   make(chan Message, 20),
			Handler: handler,
		}

		net.Register(node)
		node.StartLoop(net)
		defer node.Stop()
	}

	cs := &CriticalSection{}
	var wg sync.WaitGroup
	wg.Add(4)

	// First, issue all token requests asynchronously so the server's wait queue
	// is populated concurrently; each request prints when it is dispatched.
	for _, id := range []string{"B", "C", "D", "E"} {
		go func(id string) {
			net.SendAsync(clients[id].RequestToken("A"))
			fmt.Printf("[%s] requested token\n", id)
		}(id)
	}

	// Now start goroutines that will react to receiving the token and enter CS.
	for _, id := range nodes {
		cl := clients[id]
		go func(id string, c *MutexClient) {
			for range c.WaitChan() {
				cs.Work(id, 300*time.Millisecond, func() {
					fmt.Printf("[%s] entered critical section\n", id)
				})
				wg.Done()

				if msg, ok := c.ReleaseToken("A"); ok {
					net.SendAsync(msg)
				}
				fmt.Printf("[%s] returned token\n", id)
			}
		}(id, cl)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		time.Sleep(50 * time.Millisecond)
		fmt.Println("All nodes entered CS once")
	case <-time.After(20 * time.Second):
		t.Fatal("Deadlock detected")
	}
}
