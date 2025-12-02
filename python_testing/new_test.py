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
    "jobUid": str(uuid.uuid4()),
    "userId": "1",
    "timeout": 60,
    "nodes": {
        "testContainer": {
            "alias": "test-container",
            "testFiles": {
                "main.go": """
package main
import "fmt"
func main() {
    fmt.Println("HELLO FROM TEST")
}
"""
                ,"logfile.json": """
                [
                    {
                        "Passed": true,
                        "Name": "Login Test",
                        "Details": "User was able to log in successfully."
                    },
                    {
                        "Passed": false,
                        "Name": "Payment Test",
                        "Details": "Payment failed due to insufficient funds."
                    },
                    {
                        "Passed": true,
                        "Name": "Signup Test",
                        "Details": "New user registered correctly."
                    },
                    {
                        "Passed": false,
                        "Name": "Profile Update Test",
                        "Details": "Profile picture upload returned 500 error."
                    }
                ]
                """
            },
            "buildCommand": "go build -o testbin ./main.go",
            "entryCommand": "./testbin",
            "envs": []
        },
        "submission": {
            "submissionCode": {
                "main.go": """package main
import "fmt"
func main() {
    fmt.Println("HELLO FROM SUBMISSION")
}
"""
            },
            "buildCommand": "go build -o solution ./main.go",
            "entryCommand": "./solution",
            "globalEnvs": [],
            "replicaConfigs": [
                {
                    "alias": "replica1",
                    "envs": []
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
