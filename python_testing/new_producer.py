import json
import pika
import uuid
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

type MutexTrigger struct {
	dsnet.BaseMessage        // Type: "MutexTrigger"
	MutexID        string `json:"mutex_id"`
	// Optional: target node to initiate on; if empty, tests can set To in BaseMessage
	WorkMillis     int    `json:"work_ms"` // Simulated CS work duration
}

// ==========================================================
// 2. INTERNAL PROTOCOL (The "Logic")
// ==========================================================

// RequestCS is sent between nodes to request access to the critical section.
type RequestCS struct {
	dsnet.BaseMessage 		// Type: "RequestCS"
	MutexID      	string `json:"mutex_id"`
}

// ReplyCS is the reply to a RequestCS.
type ReplyCS struct {
	dsnet.BaseMessage // Type: "ReplyCS"
	MutexID      	string `json:"mutex_id"`
	Granted	 		bool   `json:"granted"`
}

// ReleaseCS is broadcast when a node exits the critical section,
// so waiting nodes can re-evaluate and proceed.
type ReleaseCS struct {
	dsnet.BaseMessage // Type: "ReleaseCS"
	MutexID      	string `json:"mutex_id"`
}

// ==========================================================
// 3. EXTERNAL RESULTS (The "Output")
// ==========================================================

type MutexResult struct {
	dsnet.BaseMessage
	MutexID 		string `json:"mutex_id"`
	NodeId  		string `json:"node_id"` // Node that finished its CS execution
	Success 		bool   `json:"success"` // True if mutual exclusion and progress were maintained
}
"""

test = """
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"
    
	ex "distcode/mutex"

	"github.com/distcodep7/dsnet/dsnet"
	controller "github.com/distcodep7/dsnet/testing/controller"
)

func main() {
    go controller.Serve()
    time.Sleep(2 * time.Second)
    
	tester, err := dsnet.NewNode("TESTER", "localhost:50051")
    if err != nil {
		fmt.Printf("Error creating tester node: %v\\n", err)
        return
	}
    defer tester.Close()
    
	peersJSON := os.Getenv("peers_json")
    var peers []string
    if err := json.Unmarshal([]byte(peersJSON), &peers); err != nil {
		fmt.Printf("invalid peersJSON: %v\\n", err)
        return
    }
    
    numNodes := len(peers)
    fmt.Printf("Test starting with %d nodes: %v\\n", numNodes, peers)
    
    time.Sleep(5 * time.Second) // Wait for all nodes to be ready
    
    trigger := ex.MutexTrigger{
		BaseMessage: dsnet.BaseMessage{ From: "TESTER", To: peers[0], Type: "MutexTrigger" },
		MutexID:     "TEST_MUTEX_001",
		WorkMillis:  300,
	}
	log.Printf("Sending MutexTrigger to %s...\\n", peers[0])
	tester.Send(context.Background(), peers[0], trigger)
    
    received := map[string]bool{}
	expected := map[string]bool{}
	for _, peer := range peers { expected[peer] = true }

	timeout := time.After(30 * time.Second)
	for {
		if len(received) >= len(expected) {
			log.Println("✅ TEST PASSED: All nodes completed critical section")
			return
		}
		select {
		case event := <-tester.Inbound:
			if event.Type == "MutexResult" {
				var result ex.MutexResult
				json.Unmarshal(event.Payload, &result)
				if result.Success && expected[result.NodeId] && result.MutexID == "TEST_MUTEX_001" {
					received[result.NodeId] = true
					log.Printf("Node %s completed CS (%d/%d)", result.NodeId, len(received), len(expected))
				}
			}
		case <-timeout:
			// Identify missing nodes for better error output
			missing := []string{}
			for n := range expected { if !received[n] { missing = append(missing, n) } }
			log.Fatalf("❌ TEST FAILED: Timed out waiting for MutexResult from: %v", missing)
		}
	}
}
"""

solution = """
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"
    "os"

	ex "distcode/mutex"

	"github.com/distcodep7/dsnet/dsnet"
)

func main() {
	time.Sleep(3 * time.Second)
    
    id := os.Getenv("NODE_ID")
    if id == "" {
        fmt.Println("NODE_ID environment variable not set")
        return
    }
    
    // Check if this node is the coordinator (has peers_json)
    peersJSON := os.Getenv("peers_json")
    if peersJSON != "" {
        // This is the coordinator
        fmt.Printf("Node %s starting as coordinator\\n", id)
        mutexNode := NewMutexNode(id, "")
        ctx, cancel := context.WithCancel(context.Background())
        defer cancel()
        mutexNode.Run(ctx)
    } else {
        // This is a client node
        coordinatorID := os.Getenv("COORDINATOR_ID")
        if coordinatorID == "" {
            fmt.Println("Client node missing COORDINATOR_ID")
            return
        }
        fmt.Printf("Node %s starting as client (coordinator: %s)\\n", id, coordinatorID)
        mutexNode := NewMutexNode(id, coordinatorID)
        ctx, cancel := context.WithCancel(context.Background())
        defer cancel()
        mutexNode.Run(ctx)
    }
}

type MutexNode struct{ Net *dsnet.Node; CoordinatorID string }

// Centralized coordinator state
type state struct {
	// client-side
	waitingGrant bool
	mutexID      string
	workMillis   int
	coordinatorID string

	// coordinator-side
	isCoordinator bool
	started       bool
	inCS          bool
	holder        string
	queue         []string
	completed     map[string]bool
	allNodes      []string
}

func NewMutexNode(id string, coordinatorID string) *MutexNode {
	n, err := dsnet.NewNode(id, "test:50051")
	if err != nil {
		log.Fatalf("Failed to create node %s: %v", id, err)
	}
	return &MutexNode{Net: n, CoordinatorID: coordinatorID}
}

func (en *MutexNode) Run(ctx context.Context) {
	defer en.Net.Close()
	// Detect if coordinator based on whether CoordinatorID is empty
	isCoordinator := en.CoordinatorID == ""
	coordinatorID := en.CoordinatorID
	if isCoordinator {
		coordinatorID = en.Net.ID // Coordinator references itself
	}
	
	st := state{ 
		isCoordinator: isCoordinator, 
		started: false, 
		inCS: false, 
		holder: "", 
		queue: []string{}, 
		workMillis: 300, 
		completed: map[string]bool{}, 
		allNodes: []string{}, 
		coordinatorID: coordinatorID,
	}

	for {
		select {
		case event := <-en.Net.Inbound:
			handleEvent(ctx, en, &st, event)
		case <-ctx.Done():
			return
		}
	}
}

func handleEvent(ctx context.Context, en *MutexNode, st *state, event dsnet.Event) {
	switch event.Type {
	case "MutexTrigger":
		var trig ex.MutexTrigger
		if err := json.Unmarshal(event.Payload, &trig); err != nil { return }
		st.mutexID = trig.MutexID
		if trig.WorkMillis > 0 { st.workMillis = trig.WorkMillis }

		if st.isCoordinator {
			if st.started { return }
			st.started = true
			// Coordinator reads peers list from env
			peersJSON := os.Getenv("peers_json")
			if err := json.Unmarshal([]byte(peersJSON), &st.allNodes); err != nil {
				log.Printf("Coordinator failed to parse peers_json: %v", err)
				return
			}
			// Ask all other nodes to request once
			for _, id := range st.allNodes {
				if id == en.Net.ID { continue }
				t := ex.MutexTrigger{ BaseMessage: dsnet.BaseMessage{ From: en.Net.ID, To: id, Type: "MutexTrigger" }, MutexID: st.mutexID, WorkMillis: st.workMillis }
				en.Net.Send(ctx, id, t)
			}
			// Coordinator takes its own turn first
			st.inCS = true
			doCoordinatorCS(ctx, en, st)
		} else {
			// Send request to coordinator and wait for grant
			req := ex.RequestCS{ BaseMessage: dsnet.BaseMessage{ From: en.Net.ID, To: st.coordinatorID, Type: "RequestCS" }, MutexID: st.mutexID }
			en.Net.Send(ctx, st.coordinatorID, req)
			st.waitingGrant = true
		}

	case "RequestCS":
		if !st.isCoordinator { return }
		var req ex.RequestCS
		if err := json.Unmarshal(event.Payload, &req); err != nil { return }
		handleCoordinatorRequest(ctx, en, st, req.From, req.MutexID)

	case "ReplyCS":
		if st.isCoordinator { return }
		var rep ex.ReplyCS
		if err := json.Unmarshal(event.Payload, &rep); err != nil { return }
		if !st.waitingGrant { return }
		if rep.Granted {
			st.waitingGrant = false
			doClientCS(ctx, en, st)
		} else {
			// Backoff and retry
			time.Sleep(100 * time.Millisecond)
			req := ex.RequestCS{ BaseMessage: dsnet.BaseMessage{ From: en.Net.ID, To: st.coordinatorID, Type: "RequestCS" }, MutexID: st.mutexID }
			en.Net.Send(ctx, st.coordinatorID, req)
		}

	case "ReleaseCS":
		if !st.isCoordinator { return }
		// client released; mark completion and maybe finish, else grant next
		st.inCS = false
		st.holder = ""
		st.completed[event.From] = true
		if len(st.completed) >= len(st.allNodes)-1 {
			res := ex.MutexResult{ BaseMessage: dsnet.BaseMessage{ From: en.Net.ID, To: "TESTER", Type: "MutexResult" }, MutexID: st.mutexID, NodeId: en.Net.ID, Success: true }
			en.Net.Send(ctx, "TESTER", res)
			return
		}
		grantNext(ctx, en, st)
	}
}

func handleCoordinatorRequest(ctx context.Context, en *MutexNode, st *state, from string, mutexID string) {
	// If free and no holder, grant immediately; else deny now and enqueue
	if !st.inCS && st.holder == "" {
		st.inCS = true
		st.holder = from
		rep := ex.ReplyCS{ BaseMessage: dsnet.BaseMessage{ From: en.Net.ID, To: from, Type: "ReplyCS" }, MutexID: mutexID, Granted: true }
		en.Net.Send(ctx, from, rep)
		return
	}
	// enqueue and send denial
	st.queue = append(st.queue, from)
	rep := ex.ReplyCS{ BaseMessage: dsnet.BaseMessage{ From: en.Net.ID, To: from, Type: "ReplyCS" }, MutexID: mutexID, Granted: false }
	en.Net.Send(ctx, from, rep)
}

func grantNext(ctx context.Context, en *MutexNode, st *state) {
	if st.inCS || len(st.queue) == 0 { return }
	next := st.queue[0]
	st.queue = st.queue[1:]
	st.inCS = true
	st.holder = next
	rep := ex.ReplyCS{ BaseMessage: dsnet.BaseMessage{ From: en.Net.ID, To: next, Type: "ReplyCS" }, MutexID: st.mutexID, Granted: true }
	en.Net.Send(ctx, next, rep)
}

func doClientCS(ctx context.Context, en *MutexNode, st *state) {
	// Simulate CS
	time.Sleep(time.Duration(st.workMillis) * time.Millisecond)
	// Release to coordinator
	rel := ex.ReleaseCS{ BaseMessage: dsnet.BaseMessage{ From: en.Net.ID, To: st.coordinatorID, Type: "ReleaseCS" }, MutexID: st.mutexID }
	en.Net.Send(ctx, st.coordinatorID, rel)
	// Report completion to tester
	res := ex.MutexResult{ BaseMessage: dsnet.BaseMessage{ From: en.Net.ID, To: "TESTER", Type: "MutexResult" }, MutexID: st.mutexID, NodeId: en.Net.ID, Success: true }
	en.Net.Send(ctx, "TESTER", res)
}

func doCoordinatorCS(ctx context.Context, en *MutexNode, st *state) {
	// Coordinator own CS
	time.Sleep(time.Duration(st.workMillis) * time.Millisecond)
	st.inCS = false
	st.holder = ""
	// Coordinator also reports its own completion
	res := ex.MutexResult{ BaseMessage: dsnet.BaseMessage{ From: en.Net.ID, To: "TESTER", Type: "MutexResult" }, MutexID: st.mutexID, NodeId: en.Net.ID, Success: true }
	en.Net.Send(ctx, "TESTER", res)
	grantNext(ctx, en, st)
}
"""

# --- FINAL JOB DEFINITION ---
job = {
    "jobUid": str(uuid.uuid4()),
    "userId": "1",
    "timeout": 60,
    "nodes": {
        "testContainer": {
            "alias": "test",
            "testFiles": {
                "mutex/protocol.go": protocol,
				"main.go": test,
            },
            "buildCommand": "go build -o testbin ./main.go",
            "entryCommand": "./testbin",
            "envs": [
                { "key": "peers_json", "value": "[\"replica1\", \"replica2\", \"replica3\", \"replica4\", \"replica5\"]" },
                { "key": "num_nodes", "value": "5" }
            ]
        },
        "submission": {
            "submissionCode": {
                "mutex/protocol.go": protocol,
                "main.go": solution,
            },
            "buildCommand": "go build -o solution ./main.go",
            "entryCommand": "./solution",
            "globalEnvs": [],
            "replicaConfigs": [
                {
                    "alias": "replica1",
                    "envs": [
                        {"key": "NODE_ID", "value": "replica1"},
                        {"key": "peers_json", "value": "[\"replica1\", \"replica2\", \"replica3\", \"replica4\", \"replica5\"]"}
                    ]
                },
                {
                    "alias": "replica2",
                    "envs": [
                        {"key": "NODE_ID", "value": "replica2"},
                        {"key": "COORDINATOR_ID", "value": "replica1"}
                    ]
                },
				{
                    "alias": "replica3",
                    "envs": [
                        {"key": "NODE_ID", "value": "replica3"},
                        {"key": "COORDINATOR_ID", "value": "replica1"}
                    ]
                },
                {
                    "alias": "replica4",
                    "envs": [
                        {"key": "NODE_ID", "value": "replica4"},
                        {"key": "COORDINATOR_ID", "value": "replica1"}
                    ]
                },
                {
                    "alias": "replica5",
                    "envs": [
                        {"key": "NODE_ID", "value": "replica5"},
                        {"key": "COORDINATOR_ID", "value": "replica1"}
                    ]
                }
            ]
        }
	}
}



body = json.dumps(job)

channel.basic_publish(
    exchange='',
    routing_key=QUEUE_NAME,
    body=body,
    properties=pika.BasicProperties(delivery_mode=2),
)

print("JobRequest inserted into queue.")
connection.close()
