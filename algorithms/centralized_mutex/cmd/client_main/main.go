package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"
)

type reqBody struct {
    From string `json:"from"`
}

func postJSON(url string, v interface{}) (*http.Response, error) {
    data, _ := json.Marshal(v)
    return http.Post(url, "application/json", bytes.NewReader(data))
}

func main() {
    server := flag.String("server", "http://localhost:8080", "server base URL")
    id := flag.String("id", "B", "client id")
    csMs := flag.Int("cs_ms", 200, "critical section duration (ms)")
    flag.Parse()

    client := &http.Client{Timeout: 5 * time.Second}

    // Send request
    rb := reqBody{From: *id}
    reqURL := *server + "/request"
    _, err := postJSON(reqURL, rb)
    if err != nil {
        log.Fatalf("failed to send request: %v", err)
    }
    fmt.Println("REQUEST_SENT")

    // Poll until token
    pollURL := *server + "/poll?client=" + *id
    for {
        resp, err := client.Get(pollURL)
        if err != nil {
            log.Printf("poll error: %v", err)
            time.Sleep(200 * time.Millisecond)
            continue
        }
        if resp.StatusCode == http.StatusOK {
            body, _ := ioutil.ReadAll(resp.Body)
            resp.Body.Close()
            var parsed map[string]bool
            _ = json.Unmarshal(body, &parsed)
            if parsed["token"] {
                fmt.Println("TOKEN_RECEIVED")
                break
            }
        } else {
            resp.Body.Close()
        }
        time.Sleep(100 * time.Millisecond)
    }

    // Enter critical section
    fmt.Println("ENTERED_CS")
    time.Sleep(time.Duration(*csMs) * time.Millisecond)

    // Release
    relURL := *server + "/release"
    _, err = postJSON(relURL, rb)
    if err != nil {
        log.Fatalf("failed to send release: %v", err)
    }
    fmt.Println("RELEASED")

    os.Exit(0)
}
