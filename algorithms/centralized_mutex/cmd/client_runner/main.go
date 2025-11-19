package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	cm "github.com/DistCodeP7/distcode_worker/algorithms/centralized_mutex"
)

type reqBody struct {
    From string `json:"from"`
}

func postJSON(url string, v interface{}) (*http.Response, error) {
    data, _ := json.Marshal(v)
    return http.Post(url, "application/json", bytes.NewReader(data))
}

func main() {
    server := flag.String("server", "http://worker-0:8080", "server base URL")
    id := flag.String("id", "B", "client id")
    serverID := flag.String("server-id", "A", "server node id")
    csMs := flag.Int("cs_ms", 200, "critical section duration (ms)")
    retries := flag.Int("retries", 50, "number of times to retry initial request (with backoff)")
    backoffMs := flag.Int("backoff_ms", 100, "initial backoff in milliseconds for retries")
    flag.Parse()

    // Try to read the in-container copy of the submitted client.go so we can
    // print its SHA256 and make it easy to correlate with the host-side hash.
    // Search relative to the current working directory first (walk up parents)
    // and fall back to the canonical /app location.
    var found bool
    var data []byte
    // walk up from cwd
    if cwd, err := os.Getwd(); err == nil {
        dir := cwd
        for {
            cand := filepath.Join(dir, "algorithms", "centralized_mutex", "client.go")
            if d, err := os.ReadFile(cand); err == nil {
                data = d
                found = true
                break
            }
            parent := filepath.Dir(dir)
            if parent == dir {
                break
            }
            dir = parent
        }
    }
    // fallback to /app
    if !found {
        cand := "/app/algorithms/centralized_mutex/client.go"
        if d, err := os.ReadFile(cand); err == nil {
            data = d
            found = true
        }
    }
    if found {
        sum := sha256.Sum256(data)
        fmt.Println("CONTAINER_CLIENT_GO_SHA256:", hex.EncodeToString(sum[:]))
    } else {
        log.Printf("in-container client.go not found in cwd or /app; searched from cwd and /app")
    }

    client := &http.Client{Timeout: 5 * time.Second}

    // Construct user's client using the package's constructor
    c := cm.NewMutexClient(*id, false)

    // Mark client's intent to request a token (sets waiting flag) and send request via server HTTP
    // The returned Message from RequestToken is unused here because we communicate via HTTP to the server.
    _ = c.RequestToken(*serverID)

    rb := reqBody{From: *id}
    reqURL := *server + "/request"

    // Retry the initial request a few times to handle race where server isn't listening yet.
    var lastErr error
    var resp *http.Response
    var err error
    for attempt := 1; attempt <= *retries; attempt++ {
        resp, err = postJSON(reqURL, rb)
        if err == nil {
            if resp != nil {
                resp.Body.Close()
            }
            lastErr = nil
            fmt.Println("REQUEST_SENT")
            break
        }
        lastErr = err
        // exponential-ish backoff
        sleep := time.Duration(*backoffMs*attempt) * time.Millisecond
        log.Printf("failed to send request (attempt %d/%d): %v; sleeping %v", attempt, *retries, err, sleep)
        time.Sleep(sleep)
    }
    if lastErr != nil {
        log.Fatalf("failed to send request after %d attempts: %v", *retries, lastErr)
    }

    // Poll until token
    pollURL := *server + "/poll?client=" + *id
    // Poll until token. Keep trying even if transient network errors occur.
    for {
        resp, err := client.Get(pollURL)
        if err != nil {
            log.Printf("poll error: %v", err)
            time.Sleep(200 * time.Millisecond)
            continue
        }
        if resp.StatusCode == http.StatusOK {
            // notify user's client of token
            c.OnMessage(cm.Message{From: *serverID, To: *id, Type: cm.MessageTypeToken})
            fmt.Println("TOKEN_RECEIVED")
            resp.Body.Close()
            break
        }
        resp.Body.Close()
        time.Sleep(100 * time.Millisecond)
    }

    // Wait for client's WaitChan to signal
    select {
    case <-c.WaitChan():
        fmt.Println("ENTERED_CS")
        time.Sleep(time.Duration(*csMs) * time.Millisecond)
    case <-time.After(10 * time.Second):
        log.Fatalf("timeout waiting for client to receive token")
    }

    // Release
    relURL := *server + "/release"
    _, err = postJSON(relURL, rb)
    if err != nil {
        log.Fatalf("failed to send release: %v", err)
    }
    fmt.Println("RELEASED")
}
