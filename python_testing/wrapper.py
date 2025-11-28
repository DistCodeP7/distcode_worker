import json
import pika
import uuid

RABBITMQ_URL = "amqp://guest:guest@localhost:5672/"
QUEUE_NAME = "jobs"

# Connect to RabbitMQ
params = pika.URLParameters(RABBITMQ_URL)
try:
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
except Exception as e:
    print(f"Error connecting to RabbitMQ: {e}")
    exit(1)

job = {
    "JobUID": str(uuid.uuid4()),
    "ProblemId": 1,
    "Nodes": [
        {
            "Alias": "A",
            "Files": {
                "main.go": """
                package main

                import "fmt"

                func main(){
                    fmt.Println("HELLO WORLD")
                }
                """
            },
            "Envs": [],
            "BuildCommand": "go build -o solution ./main.go",
            "EntryCommand": """
            ./wrapper -cmd ./solution
            """
        },
        {
            "Alias": "B",
            "Files": {
                "main.go": """
               package main

import (
	"context"

	"github.com/distcodep7/dsnet/testing/wrapper"
)

func main() {
	w := wrapper.NewWrapperManager(8090, "A")
	w.StartAll(context.Background())
	w.ShutdownAll(context.Background())
}
                """
            },
            "Envs": [],
            "BuildCommand": "go build -o solution ./main.go",
            "EntryCommand": "./solution"
        }
    ],
    "UserId": "1",
    "Timeout": 60,
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
