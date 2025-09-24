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
    'package main\nimport "fmt"\nfunc main() { fmt.Println(fib(20000000))}\nfunc fib(n int) int { f1:=0; f2:=1; for i := 0; i < n; i++ { f1,f2 = f2, f1+f2 }; return f1 }',
    'package main\nimport (\n\t\"fmt\"\n\t\"os\"\n)\n\nfunc IsPalindrome(s string) bool {\n\trunes := []rune(s)\n\tfor i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {\n\t\tif runes[i] != runes[j] {\n\t\t\treturn false\n\t\t}\n\t}\n\treturn true\n}\n\ntype testCase struct {\n\tinput    string\n\texpected bool\n}\n\nfunc main() {\t\n\ttestCases := []testCase{\n\t\t{\"madam\", true},\n\t\t{\"racecar\", true},\n\t\t{\"A\", true},\n\t\t{\"\", true},\n\t\t{\"hello\", false},\n\t\t{\"world\", false},\n\t\t{\"refer\", true},\n\t\t{\"golang\", false},\n\t\t{\"level\", true},\n\t\t{\"Noel sees Leon\", false}, \n\t\t{\"abacabba\", true},\n\t\t{\"abca\", false},\n\t}\n\tfailedTests := 0\n\ttotalTests := len(testCases)\n\n\tfor _, tc := range testCases {\n\t\tactual := IsPalindrome(tc.input)\n\t\tif actual != tc.expected {\n\t\t\tfailedTests++\n\t\t}\n\t}\n\n\tif failedTests == 0 {\n\t\tfmt.Printf(\"All %d tests passed successfully!\\n\", totalTests)\n\t} else {\n\t\tfmt.Fprintf(os.Stderr, \"FAILURE: %d out of %d tests failed.\\n\", failedTests, totalTests)\n\t\tos.Exit(1)\n\t}\n}\n',
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
        "ProblemId": i,
        "Code": code,
        "UserId": 1

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
