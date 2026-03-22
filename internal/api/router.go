package api

import (
	"log/slog"
	"net/http"
	"time"
)

// NewRouter builds and returns the application's HTTP mux.
//
// Routes:
//
//	GET  /health                    — liveness probe
//	POST /webhooks/payments/status  — Form3 webhook (completed/rejected)
//	GET  /payments                  — list all payments
//	GET  /payments/{id}             — get a single payment
func NewRouter(h *Handler) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", handleHealth)

	mux.HandleFunc("POST /webhooks/payments/status", h.webhookPaymentStatus)
	mux.HandleFunc("GET /payments", h.listPayments)
	mux.HandleFunc("GET /payments/{id}", h.getPayment)

	return withMiddleware(mux)
}

// handleHealth is a simple liveness probe.
func handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// withMiddleware wraps the handler with request logging.
func withMiddleware(next http.Handler) http.Handler {
	log := slog.Default().With("component", "api.router")
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &statusResponseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(rw, r)
		log.InfoContext(r.Context(), "http request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", rw.statusCode,
			"duration", time.Since(start),
		)
	})
}

// statusResponseWriter is a minimal http.ResponseWriter that captures the status code.
type statusResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (w *statusResponseWriter) WriteHeader(code int) {
	w.statusCode = code
	w.ResponseWriter.WriteHeader(code)
}
