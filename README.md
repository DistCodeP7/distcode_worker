# Go-Backend

## Quick Start

1. Start RabbitMQ server:

```bash
docker run -d --name rabbitmq \
  -p 5672:5672 -p 15672:15672 \
  rabbitmq:management
```

2. Run go application:

```bash
go run main.go
```

3. Test the message queue using Python:

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

## Setup docker environment with RabbitMQ

To set up a RabbitMQ server using Docker, you can use the following command to pull and run the RabbitMQ image with the management plugin:

```bash
docker run -d --name rabbitmq \
  -p 5672:5672 -p 15672:15672 \
  rabbitmq:management
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
