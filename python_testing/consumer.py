import json
import pika

# Configuration
RABBITMQ_URL = "amqp://guest:guest@localhost:5672/"
QUEUE_NAME = "results"

def callback(ch, method, properties, body):
    """Handle incoming messages from the results queue"""
    try:
        event = json.loads(body)
        job_id = event.get("ProblemId", "?")
        seq = event.get("SequenceIndex", "?")
        
        print(f"\n{'='*60}")
        print(f"Job {job_id} | Sequence {seq}")
        print(f"{'='*60}")
        
        for evt in event.get("Events", []):
            kind = evt.get("Kind", "unknown")
            worker_id = evt.get("WorkerId") or evt.get("worker_id", "N/A")
            
            # Shorten worker ID if it's a long hash
            if worker_id != "N/A" and len(worker_id) > 12:
                worker_id = worker_id[:12]
            
            if kind in ["stdout", "stderr"]:
                message = evt.get("Message") or evt.get("message", "")
                prefix = "OUT" if kind == "stdout" else "ERR"
                print(f"[{prefix}] {message}")
            elif kind == "error":
                message = evt.get("Message") or evt.get("message", "")
                print(f"[ERROR] {message}")
            elif kind == "metric":
                if "WorkerMetric" in evt:
                    wm = evt["WorkerMetric"]
                    print(f"[METRIC] [{worker_id}] Duration: {wm.get('DeltaTime', 'N/A')}")
                if "JobMetric" in evt:
                    jm = evt["JobMetric"]
                    print(f"[METRIC] [JOB] Scheduling: {jm.get('SchedulingDelay', 'N/A')}, "
                          f"Execution: {jm.get('ExecutionTime', 'N/A')}, "
                          f"Total: {jm.get('TotalTime', 'N/A')}")
        
        if seq == -1:
            print(f"{'='*60}")
            print(f"Job {job_id} COMPLETED")
            print(f"{'='*60}\n")
            
    except json.JSONDecodeError:
        print(f"Invalid JSON: {body}")
    except Exception as e:
        print(f"Error processing message: {e}")

# Connect to RabbitMQ
params = pika.URLParameters(RABBITMQ_URL)
connection = pika.BlockingConnection(params)
channel = connection.channel()

# Declare the queue
channel.queue_declare(queue=QUEUE_NAME, durable=True)

print(f"Listening for results on queue '{QUEUE_NAME}'...")
print("Press Ctrl+C to exit\n")

# Start consuming
channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=True)

try:
    channel.start_consuming()
except KeyboardInterrupt:
    print("\nStopping consumer...")
    channel.stop_consuming()

connection.close()
