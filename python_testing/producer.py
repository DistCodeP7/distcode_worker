import json
import pika

# Configuration
RABBITMQ_URL = "amqp://guest:guest@localhost:5672/"
QUEUE_NAME = "jobs"

# Example Go code snippets
go_snippets = [
    'package main\nimport "fmt"\nfunc main() { fmt.Println("Hello, world!") }',
    'package main\nimport "fmt"\nfunc main() { fmt.Println(2 + 2) }',
    'package main\nimport "fmt"\nfunc main() { fmt.Println("Go worker test") }',
    'package main\nimport "math"\nimport "fmt"\nfunc main() { fmt.Println(math.Sqrt(16)) }',
    'package main\nimport "fmt"\nfunc main() { for i := 0; i < 5; i++ { fmt.Println(i) } }',
    'package main\nimport "fmt"\nfunc main() { fmt.Println(len("distcode")) }',
    'package main\nimport "fmt"\nfunc main() { var x = []int{1,2,3}; fmt.Println(x) }',
    'package main\nimport "fmt"\nfunc main() { fmt.Println("Upper:", "hello world") }',
    'package main\nimport "fmt"\nfunc main() { fmt.Println("Sum:", 10+20) }',
    'package main\nimport "fmt"\nfunc main() { fmt.Println("Bool:", true && false) }',
]

# Connect to RabbitMQ
params = pika.URLParameters(RABBITMQ_URL)
connection = pika.BlockingConnection(params)
channel = connection.channel()

# Declare the queue (durable)
channel.queue_declare(queue=QUEUE_NAME, durable=True)

# Publish jobs
for i, code in enumerate(go_snippets, start=1):
    job = {
        "ID": i,
        "Code": code
    }
    body = json.dumps(job)
    channel.basic_publish(
        exchange='',
        routing_key=QUEUE_NAME,
        body=body,
        properties=pika.BasicProperties(
            delivery_mode=2,  # make message persistent
        )
    )
    print(f"Inserted job {i} into the queue.")

connection.close()
print("All jobs inserted.")
