import json
import pika
import uuid
import time # Import time for potential future debugging

# Configuration
RABBITMQ_URL = "amqp://guest:guest@localhost:5672/"
QUEUE_NAME = "jobs"

# Connect to RabbitMQ
params = pika.URLParameters(RABBITMQ_URL)
# Using a try/except block for a safer connection attempt
try:
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
except pika.exceptions.AMQPConnectionError as e:
    print(f"Error connecting to RabbitMQ: {e}")
    # Exit gracefully if connection fails
    exit(1)


# --- PROTOCOL, SOLUTION, AND GO.MOD DEFINITIONS (UNCHANGED) ---

protocol = """
package mutex

import "github.com/distcodep7/dsnet/dsnet"

// ==========================================================
// 1. EXTERNAL TRIGGERS (The "Input")
// ==========================================================

// ElectionTrigger is sent by the Tester to start the exercise.
// The student must listen for type: "ElectionTrigger"
type ElectionTrigger struct {
	dsnet.BaseMessage        // Type: "ElectionTrigger"
	ElectionID        string `json:"election_id"`
}

// ==========================================================
// 2. INTERNAL PROTOCOL (The "Logic")
// ==========================================================

// RequestVote is sent between nodes to ask for leadership.
type RequestVote struct {
	dsnet.BaseMessage     // Type: "RequestVote"
	Term              int `json:"term"`
	LastLogIndex      int `json:"last_log_index"`
}

// VoteResponse is the reply to a RequestVote.
type VoteResponse struct {
	dsnet.BaseMessage      // Type: "VoteResponse"
	Term              int  `json:"term"`
	Granted           bool `json:"granted"`
}

// ==========================================================
// 3. EXTERNAL RESULTS (The "Output")
// ==========================================================

type ElectionResult struct {
	dsnet.BaseMessage
	ElectionID string `json:"election_id"`
	Success    bool   `json:"success"`
	LeaderID   string `json:"leader_id"`
}
"""

solution = """
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"

	"distcode/exercises/mutex"

	"github.com/distcodep7/dsnet/dsnet"
)

type PaxosNode struct {
	ID    string
	Net   *dsnet.Node
	State struct {
		Term     int
		VotedFor string
	}
}

func NewPaxosNode(id, controllerAddr string) *PaxosNode {
	n, _ := dsnet.NewNode(id, controllerAddr)
	return &PaxosNode{ID: id, Net: n}
}

func (pn *PaxosNode) Run(ctx context.Context) {
	for {
		select {
		case event := <-pn.Net.Inbound:
			pn.handleEvent(ctx, event)
		case <-ctx.Done():
			return
		}
	}
}

func (pn *PaxosNode) handleEvent(ctx context.Context, event dsnet.Event) {
	switch event.Type {

	case "ElectionTrigger":
		var msg mutex.ElectionTrigger
		json.Unmarshal(event.Payload, &msg)
		log.Printf("[%s] ⚡ Trigger received. Starting election %s...", pn.ID, msg.ElectionID)

		pn.startElection(ctx, msg.ElectionID)

	case "RequestVote":
		var req mutex.RequestVote
		json.Unmarshal(event.Payload, &req)
		log.Printf("[%s] 🗳️  Received Vote Request from %s (Term: %d)", pn.ID, req.BaseMessage.From, req.Term)

		voteGranted := true

		resp := mutex.VoteResponse{
			BaseMessage: dsnet.BaseMessage{From: pn.ID, To: req.BaseMessage.From, Type: "VoteResponse"},
			Term:        req.Term,
			Granted:     voteGranted,
		}
		pn.Net.Send(ctx, req.BaseMessage.From, resp)
	case "VoteResponse":
		var resp mutex.VoteResponse
		json.Unmarshal(event.Payload, &resp)
		log.Printf("[%s] 📩 Received Vote from %s: %v", pn.ID, resp.BaseMessage.From, resp.Granted)

		if resp.Granted {

			log.Printf("[%s] 👑 I am the Leader!", pn.ID)

			result := mutex.ElectionResult{
				BaseMessage: dsnet.BaseMessage{From: pn.ID, To: "TESTER", Type: "ElectionResult"},
				Success:     true,
				LeaderID:    pn.ID,
				ElectionID:  "TEST_001",
			}
			pn.Net.Send(ctx, "TESTER", result)
		}
	}
}

func (pn *PaxosNode) startElection(ctx context.Context, _ string) {
	pn.State.Term++

	req := mutex.RequestVote{
		BaseMessage: dsnet.BaseMessage{From: pn.ID, To: "N2", Type: "RequestVote"},
		Term:        pn.State.Term,
	}
	pn.Net.Send(ctx, "N2", req)
}

func main() {
	fmt.Println("HELLO I CANT BELVIE THAT THIS ACTUALLY WOOORKS!")
	nodeID := flag.String("id", "N1", "The ID of this node")
	controllerAddr := flag.String("addr", "localhost:50051", "Address of the Network Controller")
	flag.Parse()

	log.Printf("Starting Node %s connecting to %s", *nodeID, *controllerAddr)

	node := NewPaxosNode(*nodeID, *controllerAddr)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node.Run(ctx)
}
"""

# --- FINAL JOB DEFINITION ---
job = {
    "JobUID": str(uuid.uuid4()),
    "ProblemId": 1,
    "Nodes": [
        {
            "Files": {
                "exercises/mutex/protocol.go": protocol,
                "student/solution.go": solution
            },
            "Envs": [],
			"BuildCommand": "go build -o student/solution ./student",
			"EntryCommand": "/app/tmp/student/solution -id N1 -addr controller:50051"
        }
    ],
    "UserId": "1",
    "Timeout": 300
}


body = json.dumps(job)

channel.basic_publish(
    exchange='',
    routing_key=QUEUE_NAME,
    body=body,
    properties=pika.BasicProperties(delivery_mode=2),
)

print("JobSpec inserted into queue.")
connection.close()
