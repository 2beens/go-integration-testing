package payment_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/2beens/simple-go-service/internal/outbound"
	"github.com/2beens/simple-go-service/internal/payment"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestClient_CreatePayment_OK(t *testing.T) {
	ctx := t.Context()
	ctrl := gomock.NewController(t)
	doer := NewMockhttpDoer(ctrl)

	client := payment.New("http://test", doer)
	p := outbound.NewPayment(outbound.CreatePayload{
		Amount:       1000,
		Currency:     "EUR",
		DebtorName:   "Debtor",
		CreditorName: "Creditor",
	}, "idem-key-1")

	doer.EXPECT().
		Do(gomock.AssignableToTypeOf((*http.Request)(nil))).
		DoAndReturn(func(req *http.Request) (*http.Response, error) {
			assert.Equal(t, http.MethodPost, req.Method)
			assert.Equal(t, "/payments", req.URL.Path)
			assert.Equal(t, "application/json", req.Header.Get("Content-Type"))

			var body struct {
				ID       string `json:"id"`
				Amount   int64  `json:"amount"`
				Currency string `json:"currency"`
				Debtor   string `json:"debtor"`
				Creditor string `json:"creditor"`
			}
			require.NoError(t, json.NewDecoder(req.Body).Decode(&body))
			assert.Equal(t, p.ID, body.ID, "request body must send the payment ID")
			assert.Equal(t, p.Amount, body.Amount)
			assert.Equal(t, p.Currency, body.Currency)
			assert.Equal(t, p.DebtorName, body.Debtor)
			assert.Equal(t, p.CreditorName, body.Creditor)

			return &http.Response{
				StatusCode: http.StatusCreated,
				Body:       io.NopCloser(bytes.NewReader([]byte(`{"id":"pay-123"}`))),
			}, nil
		})

	err := client.CreatePayment(ctx, p)
	require.NoError(t, err)
}

func TestClient_CreatePayment_HTTPError(t *testing.T) {
	ctx := t.Context()
	ctrl := gomock.NewController(t)
	doer := NewMockhttpDoer(ctrl)

	doer.EXPECT().
		Do(gomock.AssignableToTypeOf((*http.Request)(nil))).
		DoAndReturn(func(req *http.Request) (*http.Response, error) {
			assert.Equal(t, http.MethodPost, req.Method)
			assert.Equal(t, "/payments", req.URL.Path)
			assert.Equal(t, "application/json", req.Header.Get("Content-Type"))
			return &http.Response{StatusCode: http.StatusInternalServerError, Body: io.NopCloser(bytes.NewReader(nil))}, nil
		})

	client := payment.New("http://test", doer)
	p := outbound.NewPayment(outbound.CreatePayload{
		Amount:       200,
		Currency:     "GBP",
		DebtorName:   "Debtor",
		CreditorName: "Creditor",
	}, "idem-key-2")

	err := client.CreatePayment(ctx, p)
	require.Error(t, err)
}

func TestClient_CreatePayment_UnreachableServer(t *testing.T) {
	ctx := t.Context()
	ctrl := gomock.NewController(t)
	doer := NewMockhttpDoer(ctrl)

	doer.EXPECT().
		Do(gomock.AssignableToTypeOf((*http.Request)(nil))).
		DoAndReturn(func(req *http.Request) (*http.Response, error) {
			assert.Equal(t, http.MethodPost, req.Method)
			assert.Equal(t, "/payments", req.URL.Path)
			assert.Equal(t, "application/json", req.Header.Get("Content-Type"))
			return nil, io.EOF
		})

	client := payment.New("http://test", doer)
	p := outbound.NewPayment(outbound.CreatePayload{
		Amount:       100,
		Currency:     "EUR",
		DebtorName:   "Debtor",
		CreditorName: "Creditor",
	}, "idem-key-3")

	err := client.CreatePayment(ctx, p)
	require.Error(t, err)
}
