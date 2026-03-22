package api_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/2beens/simple-go-service/internal/api"
	"github.com/2beens/simple-go-service/internal/outbound"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestHandler_WebhookPaymentStatus_OK(t *testing.T) {
	ctx := t.Context()
	ctrl := gomock.NewController(t)
	svc := NewMockpaymentService(ctrl)

	svc.EXPECT().
		UpdateStatusFromWebhook(gomock.Any(), "pay-1", outbound.StatusCompleted).
		Return(outbound.Payment{ID: "pay-1", Status: outbound.StatusCompleted}, nil)

	router := api.NewRouter(api.NewHandler(svc))

	body, _ := json.Marshal(map[string]string{"payment_id": "pay-1", "status": "completed"})
	req := httptest.NewRequest(http.MethodPost, "/webhooks/payments/status", bytes.NewReader(body)).WithContext(ctx)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_WebhookPaymentStatus_InvalidBody(t *testing.T) {
	ctx := t.Context()
	ctrl := gomock.NewController(t)

	router := api.NewRouter(api.NewHandler(NewMockpaymentService(ctrl)))

	req := httptest.NewRequest(http.MethodPost, "/webhooks/payments/status", bytes.NewReader([]byte("{"))).WithContext(ctx)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_WebhookPaymentStatus_InvalidStatus(t *testing.T) {
	ctx := t.Context()
	ctrl := gomock.NewController(t)

	router := api.NewRouter(api.NewHandler(NewMockpaymentService(ctrl)))

	body, _ := json.Marshal(map[string]string{"payment_id": "pay-1", "status": "pending"})
	req := httptest.NewRequest(http.MethodPost, "/webhooks/payments/status", bytes.NewReader(body)).WithContext(ctx)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_WebhookPaymentStatus_NotFound(t *testing.T) {
	ctx := t.Context()
	ctrl := gomock.NewController(t)
	svc := NewMockpaymentService(ctrl)

	svc.EXPECT().
		UpdateStatusFromWebhook(gomock.Any(), "pay-missing", outbound.StatusCompleted).
		Return(outbound.Payment{}, outbound.ErrNotFound)

	router := api.NewRouter(api.NewHandler(svc))

	body, _ := json.Marshal(map[string]string{"payment_id": "pay-missing", "status": "completed"})
	req := httptest.NewRequest(http.MethodPost, "/webhooks/payments/status", bytes.NewReader(body)).WithContext(ctx)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_GetPayment_NotFound(t *testing.T) {
	ctx := t.Context()
	ctrl := gomock.NewController(t)
	svc := NewMockpaymentService(ctrl)

	svc.EXPECT().
		Get(gomock.Any(), "missing").
		Return(outbound.Payment{}, outbound.ErrNotFound)

	router := api.NewRouter(api.NewHandler(svc))

	req := httptest.NewRequest(http.MethodGet, "/payments/missing", nil).WithContext(ctx)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_GetPayment_OK(t *testing.T) {
	ctx := t.Context()
	ctrl := gomock.NewController(t)
	want := outbound.Payment{ID: "pay-1", Amount: 500, Currency: "USD", Status: outbound.StatusCreated}
	svc := NewMockpaymentService(ctrl)

	svc.EXPECT().
		Get(gomock.Any(), "pay-1").
		Return(want, nil)

	router := api.NewRouter(api.NewHandler(svc))

	req := httptest.NewRequest(http.MethodGet, "/payments/pay-1", nil).WithContext(ctx)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	var got outbound.Payment
	err := json.NewDecoder(rec.Body).Decode(&got)
	require.NoError(t, err)
	assert.Equal(t, want.ID, got.ID)
	assert.Equal(t, want.Amount, got.Amount)
}

func TestHandler_ListPayments_Empty(t *testing.T) {
	ctx := t.Context()
	ctrl := gomock.NewController(t)
	svc := NewMockpaymentService(ctrl)

	svc.EXPECT().
		List(gomock.Any()).
		Return(nil, nil)

	router := api.NewRouter(api.NewHandler(svc))

	req := httptest.NewRequest(http.MethodGet, "/payments", nil).WithContext(ctx)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	var payments []outbound.Payment
	err := json.NewDecoder(rec.Body).Decode(&payments)
	require.NoError(t, err)
	assert.NotNil(t, payments)
	assert.Empty(t, payments)
}
