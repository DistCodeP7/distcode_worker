package centralized_mutex

import "sync"

// Centralised mutex server.
type MutexServer struct {
	mu        sync.Mutex
	NodeID    string
	hasToken  bool
	waitQueue []string
}

func NewMutexServer(nodeID string, hasToken bool) *MutexServer {
	return &MutexServer{
		NodeID:    nodeID,
		hasToken:  hasToken,
		waitQueue: []string{},
	}
}

func (s *MutexServer) OnMessage(msg Message) []Message {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch msg.Type {

	case MessageTypeRequestToken:
		if s.hasToken {
			s.hasToken = false
			return []Message{
				{From: s.NodeID, To: msg.From, Type: MessageTypeToken},
			}
		}
		s.waitQueue = append(s.waitQueue, msg.From)

	case MessageTypeReleaseToken:
		if len(s.waitQueue) > 0 {
			next := s.waitQueue[0]
			s.waitQueue = s.waitQueue[1:]
			return []Message{
				{From: s.NodeID, To: next, Type: MessageTypeToken},
			}
		}
		s.hasToken = true
	}

	return nil
}
