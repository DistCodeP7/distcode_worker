package centralized_mutex

// CombinedHandler routes messages to either the server (A) or client.
type CombinedHandler struct {
	Server *MutexServer
	Client MutexClientInterface
}

func (h *CombinedHandler) OnMessage(msg Message) []Message {

	switch msg.Type {

	case MessageTypeRequestToken, MessageTypeReleaseToken:
		if h.Server != nil {
			return h.Server.OnMessage(msg)
		}

	case MessageTypeToken:
		if h.Client != nil {
			return h.Client.OnMessage(msg)
		}
	}

	return nil
}
