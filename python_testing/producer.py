import json
import pika

# Configuration
RABBITMQ_URL = "amqp://guest:guest@localhost:5672/"
QUEUE_NAME = "jobs"

# Example Go code snippets

networked_example =  [
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
    ],


varying_execution_time_examples =  [   # CPU-bound recursive Fibonacci (slow)
    ["""package main
import "fmt"

func fib(n int) int {
    if n < 2 {
        return n
    }
    return fib(n-1) + fib(n-2)
}

func main() {
    fmt.Println("Fib(40) =", fib(40))
}"""],

    # CPU-bound iterative Fibonacci (heavy loop)
    ["""package main
import "fmt"

func fib(n int) int {
    f1, f2 := 0, 1
    for i := 0; i < n; i++ {
        f1, f2 = f2, f1+f2
    }
    return f1
}

func main() {
    fmt.Println("Fib(100000000) =", fib(100000000))
}"""],

    # Artificial busy loop to burn CPU
    ["""package main
import "fmt"

func main() {
    sum := 0
    for i := 0; i < 1e9; i++ {
        sum += i
    }
    fmt.Println("Sum:", sum)
}"""],

    # Slow I/O-bound (sleep)
    ["""package main
import (
    "fmt"
    "time"
)

func main() {
    fmt.Println("Sleeping 10s...")
    time.Sleep(10 * time.Second)
    fmt.Println("Done sleeping")
}"""],

    # Mixed: sleep and then CPU
    ["""package main
import (
    "fmt"
    "time"
)

func fib(n int) int {
    if n < 2 {
        return n
    }
    return fib(n-1) + fib(n-2)
}

func main() {
    fmt.Println("Start slow job")
    time.Sleep(5 * time.Second)
    fmt.Println("Fib(38) =", fib(38))
}"""],
 ]


dsnet = [
    """
 package main

import (
    "log"
    // Import the package and give it an alias to access its exported functions
    dsnet "github.com/distcode/dsnet/dsnet" 
)

func main() {
    log.Println("Attempting to connect...")
    // Use the package-qualified function name: dsnet.Connect
    _, err := dsnet.Connect("localhost:50051", "nodeA")
    
    if err != nil {
        return
    }
    log.Println("Successfully connected or connection process finished.")
}
    """
]

go_snippets = [
   #networked_example,
   dsnet,
   #[ 'package main\nimport "fmt"\nfunc main() { fmt.Println("Hello, world!") }', 'package main\nimport "fmt"\nfunc main() { fmt.Println("World, world!") }'],
   #[ 'package main\nimport ("fmt"; "time")\nfunc main() { for i := 1; i <= 5; i++ { fmt.Println("Count:", i); time.Sleep(1 * time.Second) } }', 'package main\nimport "fmt"\nfunc main() { for i := 1; i <= 3; i++ { fmt.Println("Number:", i) } }'],
   #[ 'package main\nimport "fmt"\nfunc main() { fmt.Println(2 + 2) }'],
   #[ 'package main\nimport "fmt"\nfunc main() { fmt.Println("Go worker test") }'],
   #[ 'package main\nimport "math"\nimport "fmt"\nfunc main() { fmt.Println(math.Sqrt(16)) }'],
   #[ 'package main\nimport "fmt"\nfunc main() { for i := 0; i < 5; i++ { fmt.Println(i) } }'],
   #[ 'package main\nimport "fmt"\nfunc main() { fmt.Println(len("distcode")) }'],
   #[ 'package main\nimport "fmt"\nfunc main() { var x = []int{1,2,3}; fmt.Println(x) }'],
   #[ 'package main\nimport "fmt"\nfunc main() { fmt.Println("Upper:", "hello world") }'],
   #[ 'package main\nimport "fmt"\nfunc main() { fmt.Println("Sum:", 10+20) }'],
   #[ 'package main\nimport "fmt"\nfunc main() { fmt.Println("Bool:", true && false) }'],
   #[ 'package main\nimport "fmt"\nfunc main() { fib(20000)}\nfunc fib(n int) int { f1:=0; f2:=1; for i := 0; i < n; i++ { f1,f2 = f2, f1+f2 }; return f1 }'],
   #[ 'package main\nimport (\n\t\"fmt\"\n\t\"os\"\n)\n\nfunc IsPalindrome(s string) bool {\n\trunes := []rune(s)\n\tfor i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {\n\t\tif runes[i] != runes[j] {\n\t\t\treturn false\n\t\t}\n\t}\n\treturn true\n}\n\ntype testCase struct {\n\tinput    string\n\texpected bool\n}\n\nfunc main() {\t\n\ttestCases := []testCase{\n\t\t{\"madam\", true},\n\t\t{\"racecar\", true},\n\t\t{\"A\", true},\n\t\t{\"\", true},\n\t\t{\"hello\", false},\n\t\t{\"world\", false},\n\t\t{\"refer\", true},\n\t\t{\"golang\", false},\n\t\t{\"level\", true},\n\t\t{\"Noel sees Leon\", false}, \n\t\t{\"abacabba\", true},\n\t\t{\"abca\", false},\n\t}\n\tfailedTests := 0\n\ttotalTests := len(testCases)\n\n\tfor _, tc := range testCases {\n\t\tactual := IsPalindrome(tc.input)\n\t\tif actual != tc.expected {\n\t\t\tfailedTests++\n\t\t}\n\t}\n\n\tif failedTests == 0 {\n\t\tfmt.Printf(\"All %d tests passed successfully!\\n\", totalTests)\n\t} else {\n\t\tfmt.Fprintf(os.Stderr, \"FAILURE: %d out of %d tests failed.\\n\", failedTests, totalTests)\n\t\tos.Exit(1)\n\t}\n}\n'],
   
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
        "UserId": 1,
        "TimeoutLimit": 30  # seconds

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
