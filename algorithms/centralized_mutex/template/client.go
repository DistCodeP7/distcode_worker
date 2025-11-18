package template

/*
import "sync"

// MutexClient implements MutexClientInterface for the centralised algorithm.
type MutexClient struct {
	mu        sync.Mutex
	NodeID    string
	hasToken  bool
	waiting   bool
	csEntryCh chan struct{}
}

func NewMutexClient(nodeID string, hasToken bool) *MutexClient {
	return &MutexClient{
		NodeID:    nodeID,
		hasToken:  hasToken,
		csEntryCh: make(chan struct{}, 1),
	}
}

func (c *MutexClient) RequestToken(serverID string) Message {
	c.mu.Lock()
	c.waiting = true
	c.mu.Unlock()

	return Message{
		From: c.NodeID,
		To:   serverID,
		Type: MessageTypeRequestToken,
	}
}

func (c *MutexClient) ReleaseToken(serverID string) (Message, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.hasToken {
		return Message{}, false
	}

	c.hasToken = false
	return Message{
		From: c.NodeID,
		To:   serverID,
		Type: MessageTypeReleaseToken,
	}, true
}

func (c *MutexClient) OnMessage(msg Message) []Message {
	c.mu.Lock()
	defer c.mu.Unlock()

	if msg.Type == MessageTypeToken {
		c.hasToken = true

		if c.waiting {
			c.waiting = false
			select {
			case c.csEntryCh <- struct{}{}:
			default:
			}
		}
	}

	return nil
}

func (c *MutexClient) WaitChan() <-chan struct{} {
	return c.csEntryCh
}
*/
