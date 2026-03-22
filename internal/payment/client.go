// Package payment provides an HTTP client for the Form3 payment API (simulated).
package payment

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/2beens/simple-go-service/internal/outbound"
)

//go:generate mockgen -destination=client_mocks_test.go -package=payment_test -typed=true . httpDoer

// httpDoer executes HTTP requests. Defined here (consumer) for testability.
type httpDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// Client is an HTTP client that creates payments via the Form3 API.
type Client struct {
	baseURL string
	doer    httpDoer
	log     *slog.Logger
}

// New creates a new Form3 payment Client.
// If cfg.Timeout is zero, a 10-second default is used.
func New(baseURL string, doer httpDoer) *Client {
	return &Client{
		baseURL: baseURL,
		doer:    doer,
		log:     slog.Default().With("component", "payment.client"),
	}
}

// createRequest is the JSON body sent to Form3 to create a payment.
// We send our payment ID (UUID) so Form3 can reference it in webhook callbacks.
type createRequest struct {
	ID       string `json:"id"`
	Amount   int64  `json:"amount"`
	Currency string `json:"currency"`
	Debtor   string `json:"debtor"`
	Creditor string `json:"creditor"`
}

// createResponse is the JSON body received from Form3 (optional; 201 is sufficient).
type createResponse struct {
	ID string `json:"id"`
}

// CreatePayment POSTs the payment to the Form3 API. The payment ID is set by us (UUID)
// and sent in the request so Form3 can use it when calling our webhook.
// Returns an error if the API call fails or returns a non-2xx status.
func (c *Client) CreatePayment(ctx context.Context, p outbound.Payment) error {
	body, err := json.Marshal(createRequest{
		ID:       p.ID,
		Amount:   p.Amount,
		Currency: p.Currency,
		Debtor:   p.DebtorName,
		Creditor: p.CreditorName,
	})
	if err != nil {
		return fmt.Errorf("marshal create payment request: %w", err)
	}

	url := c.baseURL + "/payments"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create payment request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.doer.Do(req)
	if err != nil {
		return fmt.Errorf("call Form3 API: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			c.log.WarnContext(ctx, "close Form3 response body", "err", err)
		}
	}()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Form3 API returned status: %d", resp.StatusCode)
	}

	// Optionally decode response; we only need 201.
	if resp.StatusCode == http.StatusCreated && resp.Body != nil {
		var cr createResponse
		if err := json.NewDecoder(resp.Body).Decode(&cr); err != nil {
			// Non-fatal: we got 201
			c.log.DebugContext(ctx, "decode Form3 create response", "err", err)
		} else if cr.ID != "" {
			c.log.DebugContext(ctx, "Form3 create response", "id", cr.ID)
		}
	}

	c.log.InfoContext(ctx, "payment created with Form3", "payment_id", p.ID)
	return nil
}
