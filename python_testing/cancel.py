import json
import uuid
import pika
import time

# Configuration
RABBITMQ_URL = "amqp://guest:guest@localhost:5672/"
JOB_QUEUE = "jobs"
CANCEL_QUEUE = "jobs_cancel"

# CocreateJobContextnnect to RabbitMQ
params = pika.URLParameters(RABBITMQ_URL)
connection = pika.BlockingConnection(params)
channel = connection.channel()

# Declare queues (durable)
channel.queue_declare(queue=JOB_QUEUE, durable=True)
channel.queue_declare(queue=CANCEL_QUEUE, durable=True)
job_uid = str(uuid.uuid4())
# Publish a job
job = {
    "JobUID": job_uid,
    "ProblemId": 1,
    "Nodes": [
        {
            "Files": {
                "main.go": """
                package main

                import (
                    "fmt"
                    "time"
                )

                func main(){
                    fmt.Println("HELLO WORLD")
                    time.Sleep(60 * time.Second)
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
channel.basic_publish(
    exchange='',
    routing_key=JOB_QUEUE,
    body=json.dumps(job),
    properties=pika.BasicProperties(delivery_mode=2)
)
print(f"Inserted job {job_uid} into the queue.")

# Wait a few seconds to let the worker start
time.sleep(3)

# Send cancel request
cancel_msg = {"JobUID": job_uid}
channel.basic_publish(
    exchange='',
    routing_key=CANCEL_QUEUE,
    body=json.dumps(cancel_msg),
    properties=pika.BasicProperties(delivery_mode=2)
)
print(f"Sent cancel request for job {job_uid}")

# Close connection
connection.close()
