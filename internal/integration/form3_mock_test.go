package integration_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
)

// form3CreateRequest mirrors the JSON body sent to Form3 (see internal/payment/client.go).
type form3CreateRequest struct {
	ID       string `json:"id"`
	Amount   int64  `json:"amount"`
	Currency string `json:"currency"`
	Debtor   string `json:"debtor"`
	Creditor string `json:"creditor"`
}

// form3RequestRecorder records POST /payments requests for assertion in tests.
type form3RequestRecorder struct {
	mu       sync.Mutex
	requests []form3CreateRequest
}

func newForm3RequestRecorder() *form3RequestRecorder {
	return &form3RequestRecorder{}
}

func (r *form3RequestRecorder) record(body []byte) {
	var req form3CreateRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.requests = append(r.requests, req)
}

// Requests returns a copy of recorded Form3 create requests.
func (r *form3RequestRecorder) Requests() []form3CreateRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.requests)
}

func newForm3MockServer(recorder *form3RequestRecorder) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Log the received request.
		fmt.Printf(" --> form3 mock server received request: %s %s\n", r.Method, r.URL.Path)

		if r.URL.Path != "/payments" || r.Method != http.MethodPost {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		if r.Body != nil {
			body, err := io.ReadAll(r.Body)
			if err == nil {
				recorder.record(body)
			}
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		fmt.Fprint(w, `{"id":"form3-mock-id"}`)
	}))
}
