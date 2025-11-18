package centralized_mutex

// MessageType enumerates all possible logical operations.
type MessageType int

const (
	MessageTypeRequestToken MessageType = iota
	MessageTypeReleaseToken
	MessageTypeToken
)

// Message represents a simple asynchronous message between nodes.
type Message struct {
	From string
	To   string
	Type MessageType
	Body interface{}
}
