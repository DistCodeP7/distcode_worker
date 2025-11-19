package centralized_mutex

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type requestBody struct {
	From string `json:"from"`
}

// TestCentralizedMutex runs the centralized-mutex algorithm against the HTTP
// handlers (httptest.Server). This combines the original in-memory test with
// the HTTP transport layer so that a single unit test verifies correctness
// over the network wiring.
func TestCentralizedMutex(t *testing.T) {
	fmt.Println("=== START TestCentralizedMutex ===")
	server := NewMutexServer("A", true)

	delivered := make(map[string]bool)
	var mu sync.Mutex

	mux := http.NewServeMux()

	mux.HandleFunc("/request", func(w http.ResponseWriter, r *http.Request) {
		var req requestBody
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		fmt.Printf("handler /request: from=%s\n", req.From)
		outs := server.OnMessage(Message{From: req.From, To: "A", Type: MessageTypeRequestToken})
		mu.Lock()
		for _, m := range outs {
			if m.Type == MessageTypeToken {
				delivered[m.To] = true
			}
		}
		mu.Unlock()

		mu.Lock()
		have := delivered[req.From]
		mu.Unlock()
		if have {
			json.NewEncoder(w).Encode(struct{Token bool `json:"token"`}{Token: true})
			return
		}
		w.WriteHeader(http.StatusAccepted)
	})

	mux.HandleFunc("/release", func(w http.ResponseWriter, r *http.Request) {
		var req requestBody
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		fmt.Printf("handler /release: from=%s\n", req.From)
		outs := server.OnMessage(Message{From: req.From, To: "A", Type: MessageTypeReleaseToken})
		mu.Lock()
		for _, m := range outs {
			if m.Type == MessageTypeToken {
				delivered[m.To] = true
			}
		}
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("/poll", func(w http.ResponseWriter, r *http.Request) {
		client := r.URL.Query().Get("client")
		if client == "" {
			http.Error(w, "missing client", http.StatusBadRequest)
			return
		}
		fmt.Printf("handler /poll: client=%s\n", client)
		mu.Lock()
		have := delivered[client]
		if have {
			delete(delivered, client)
		}
		mu.Unlock()
		if have {
			json.NewEncoder(w).Encode(struct{Token bool `json:"token"`}{Token: true})
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()
	fmt.Printf("httptest server started: %s\n", ts.URL)

	ids := []string{"B", "C", "D", "E"}
	var enteredAtomic int32
	var wg sync.WaitGroup
	wg.Add(len(ids))
	errCh := make(chan string, len(ids))

	// start clients
	for _, id := range ids {
		go func(id string) {
			defer wg.Done()
			c := NewMutexClient(id, false)

			// mark intent locally
			fmt.Printf("client %s: RequestToken() local intent set\n", id)
			_ = c.RequestToken("A")

			client := &http.Client{Timeout: 2 * time.Second}
			rb := requestBody{From: id}
			fmt.Printf("client %s: POST /request -> %s\n", id, ts.URL+"/request")
			_, _ = client.Post(ts.URL+"/request", "application/json", bytesReader(rb))

			// poll until token
			pollURL := ts.URL + "/poll?client=" + id
			for {
				resp, err := client.Get(pollURL)
				if err == nil {
					if resp.StatusCode == http.StatusOK {
						fmt.Printf("client %s: poll returned token\n", id)
						c.OnMessage(Message{From: "A", To: id, Type: MessageTypeToken})
						resp.Body.Close()
						break
					}
					resp.Body.Close()
				}
				time.Sleep(20 * time.Millisecond)
			}

			// wait for WaitChan then enter CS and release
			fmt.Printf("client %s: waiting on WaitChan\n", id)
			select {
			case <-c.WaitChan():
				fmt.Printf("client %s: entered WaitChan, entering CS\n", id)
				if atomic.AddInt32(&enteredAtomic, 1) != 1 {
					errCh <- "mutual exclusion violated: more than one client in CS"
					return
				}
				time.Sleep(20 * time.Millisecond)
				fmt.Printf("client %s: leaving CS\n", id)
				atomic.AddInt32(&enteredAtomic, -1)
			case <-time.After(3 * time.Second):
				errCh <- "timeout waiting for token"
				return
			}

			// release
			fmt.Printf("client %s: POST /release -> %s\n", id, ts.URL+"/release")
			client.Post(ts.URL+"/release", "application/json", bytesReader(rb))
		}(id)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		select {
		case e := <-errCh:
			t.Fatalf("test failed: %s", e)
		default:
			fmt.Println("=== END TestCentralizedMutex: success ===")
			// success
		}
	case <-time.After(5 * time.Second):
		t.Fatal("test timed out")
	}
}

// helper to marshal a value into an io.Reader without importing bytes repeatedly
func bytesReader(v interface{}) *bytes.Reader {
	b, _ := json.Marshal(v)
	return bytes.NewReader(b)
}

