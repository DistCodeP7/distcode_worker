import json
import pika
import time

# Configuration
RABBITMQ_URL = "amqp://guest:guest@localhost:5672/"
JOB_QUEUE = "jobs"
CANCEL_QUEUE = "jobs_cancel"
networked_example = [
    '''
    package main

    import (
        "fmt"
        "net/http"
    )

    func main() {
        // Worker 0 runs an HTTP server
        http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
            fmt.Fprintf(w, "Hello from worker 0")
        })
        fmt.Println("Worker 0 listening on :8080")
        http.ListenAndServe(":8080", nil)
    }
    ''',
    '''
    package main

    import (
        "fmt"
        "net/http"
        "time"
    )

    func main() {
        // Give worker 0 a second to start
        time.Sleep(time.Second)

        // Worker 1 sends HTTP request to worker 0
        resp, err := http.Get("http://worker-0:8080")
        if err != nil {
            fmt.Println("Error connecting:", err)
            return
        }
        defer resp.Body.Close()
        fmt.Println("Worker 1 received:", resp.Status)
    }
    '''
]

# Connect to RabbitMQ
params = pika.URLParameters(RABBITMQ_URL)
connection = pika.BlockingConnection(params)
channel = connection.channel()

# Declare queues (durable)
channel.queue_declare(queue=JOB_QUEUE, durable=True)
channel.queue_declare(queue=CANCEL_QUEUE, durable=True)

# Publish a job
job_uid = "123e4567-e89b-12d3-a456-426614174000"
job = {
    "JobUID": job_uid,
    "ProblemId": 5,
    "Code": networked_example,
    "UserId": 5,
    "TimeoutLimit": 30
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
