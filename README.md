# Go-Backend

## Getting Started

1. Start RabbitMQ server from the docker-compose repo:

```bash
git clone https://github.com/DistCodeP7/docker-compose.git docker-compose
cd docker-compose
docker-compose up -d
```

1. Run go application:

```bash
go run main.go
```

2. Test the message queue using Python:

```bash
cd python_testing
python3 -m venv venv
source venv/bin/activate
pip install pika
python producer.py
```

## Run the Go application

1. Run the application run:

```bash
go run main.go
```

2. Run tests:

```bash
go test ./...
```

## Python Message Queue script

In order to test the worker you can publish some code messages to the RabbitMQ queue using the provided Python script. Make sure you have Python and the `pika` library installed.

1. Setup venv and install dependencies:
   From the python_testing directory run:

   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install pika
   ```

2. Run the script to publish messages:

   ```bash
   python producer.py
   ```
