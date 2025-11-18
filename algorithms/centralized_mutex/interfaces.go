package centralized_mutex

// MessageHandler receives a message and may return outgoing messages.
type MessageHandler interface {
	OnMessage(msg Message) []Message
}

// MutexClientInterface defines the surface that user-submitted clients must implement.
type MutexClientInterface interface {
	RequestToken(serverID string) Message
	ReleaseToken(serverID string) (Message, bool)
	OnMessage(msg Message) []Message
	WaitChan() <-chan struct{}
}
