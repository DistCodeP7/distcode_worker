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




# --- FINAL JOB DEFINITION ---
job = {
    "JobUID": str(uuid.uuid4()),
    "ProblemId": 1,
    "Nodes": [
        {
            "Files": {
                "main.go": """
                package main

                import "fmt"

                func main(){
                    fmt.Println("HELLO WORLD")
                }
                """,
            },
            "Envs": [],
			"BuildCommand": "go build -o solution ./main.go",
			"EntryCommand": "./solution"
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
