package types

import (
	"encoding/json"
	"errors"
)

type Message struct {
	ID      int    `json:"id"`
	Payload string `json:"payload"`
}

func ParseMessage(data []byte) (*Message, error) {
	var msg Message
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	if msg.ID == 0 || msg.Payload == "" {
		return nil, errors.New("missing required fields")
	}
	return &msg, nil
}
