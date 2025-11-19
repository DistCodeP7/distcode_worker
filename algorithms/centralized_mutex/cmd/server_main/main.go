package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"sync"

	cm "github.com/DistCodeP7/distcode_worker/algorithms/centralized_mutex"
)

type requestBody struct {
    From string `json:"from"`
}

type pollResp struct {
    Token bool `json:"token"`
}

func main() {
    addr := flag.String("addr", ":8080", "server listen address")
    id := flag.String("id", "A", "server node id")
    flag.Parse()

    server := cm.NewMutexServer(*id, true)
    delivered := make(map[string]bool)
    var mu sync.Mutex

    http.HandleFunc("/request", func(w http.ResponseWriter, r *http.Request) {
        var req requestBody
        if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
            http.Error(w, "bad request", http.StatusBadRequest)
            return
        }
        outs := server.OnMessage(cm.Message{From: req.From, To: *id, Type: cm.MessageTypeRequestToken})
        mu.Lock()
        for _, m := range outs {
            if m.Type == cm.MessageTypeToken {
                delivered[m.To] = true
            }
        }
        mu.Unlock()

        // If we delivered a token to the requester, respond with token:true
        mu.Lock()
        have := delivered[req.From]
        mu.Unlock()
        if have {
            json.NewEncoder(w).Encode(pollResp{Token: true})
            return
        }
        w.WriteHeader(http.StatusAccepted)
    })

    http.HandleFunc("/release", func(w http.ResponseWriter, r *http.Request) {
        var req requestBody
        if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
            http.Error(w, "bad request", http.StatusBadRequest)
            return
        }
        outs := server.OnMessage(cm.Message{From: req.From, To: *id, Type: cm.MessageTypeReleaseToken})
        mu.Lock()
        for _, m := range outs {
            if m.Type == cm.MessageTypeToken {
                delivered[m.To] = true
            }
        }
        mu.Unlock()
        w.WriteHeader(http.StatusOK)
    })

    http.HandleFunc("/poll", func(w http.ResponseWriter, r *http.Request) {
        client := r.URL.Query().Get("client")
        if client == "" {
            http.Error(w, "missing client", http.StatusBadRequest)
            return
        }
        mu.Lock()
        have := delivered[client]
        if have {
            delete(delivered, client)
        }
        mu.Unlock()
        if have {
            json.NewEncoder(w).Encode(pollResp{Token: true})
            return
        }
        w.WriteHeader(http.StatusNoContent)
    })

        log.Printf("Starting centralized-mutex HTTP server on %s (id=%s)", *addr, *id)
        log.Fatal(http.ListenAndServe(*addr, nil))
    }
