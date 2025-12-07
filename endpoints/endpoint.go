package endpoints

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/DistCodeP7/distcode_worker/endpoints/health"
)

// HTTPServer wraps an HTTP listener serving JSON metrics.
type HTTPServer struct {
	addr           string
	metrics        Manager
	healthRegister *health.HealthServiceRegister
	server         *http.Server
}

// NewHTTPServer creates a new server instance.
func NewHTTPServer(addr string, metrics Manager, healthRegister *health.HealthServiceRegister) *HTTPServer {
	return &HTTPServer{
		addr:           addr,
		metrics:        metrics,
		healthRegister: healthRegister,
	}
}

// Run starts the server and shuts down gracefully when ctx is canceled.
func (s *HTTPServer) Run(ctx context.Context) {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(s.metrics.AggregateJSON())
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		overallHealthy := s.healthRegister.OverallStatus()

		if overallHealthy {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status":"healthy"}`))
			return
		}

		healthStatus := s.healthRegister.CheckAll()
		w.WriteHeader(http.StatusServiceUnavailable)
		encoder := json.NewEncoder(w)
		if err := encoder.Encode(healthStatus); err != nil {
			log.Printf("Failed to encode health status: %v", err)
		}
	})

	s.server = &http.Server{
		Addr:    s.addr,
		Handler: mux,
	}

	go func() {
		log.Printf("Serving metrics at http://%s/metrics", s.addr)
		log.Printf("Serving health at http://%s/health", s.addr)
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	// Wait for context cancellation
	<-ctx.Done()
	log.Printf("Shutting down metrics server at %s...", s.addr)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.server.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	} else {
		log.Printf("Metrics server shut down cleanly")
	}
}
