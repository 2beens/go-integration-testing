package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/2beens/simple-go-service/internal/outbound"
)

//go:generate mockgen -destination=handler_mocks_test.go -package=api_test -typed=true . paymentService

// paymentService is the subset of outbound.Service consumed by the HTTP handlers. Defined here (consumer) per Go idiom.
type paymentService interface {
	Get(ctx context.Context, id string) (outbound.Payment, error)
	List(ctx context.Context) ([]outbound.Payment, error)
	UpdateStatusFromWebhook(ctx context.Context, paymentID string, status outbound.Status) (outbound.Payment, error)
}

// Handler holds the HTTP handlers for payments and webhooks.
type Handler struct {
	svc paymentService
	log *slog.Logger
}

// NewHandler creates a Handler.
func NewHandler(svc paymentService) *Handler {
	return &Handler{
		svc: svc,
		log: slog.Default().With("component", "api.handler"),
	}
}

// WebhookPaymentStatusRequest is the body for POST /webhooks/payments/status.
type WebhookPaymentStatusRequest struct {
	PaymentID string `json:"payment_id"`
	Status    string `json:"status"`
}

// webhookPaymentStatus handles POST /webhooks/payments/status (Form3 callback).
func (h *Handler) webhookPaymentStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req WebhookPaymentStatusRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.PaymentID == "" {
		writeError(w, http.StatusBadRequest, "missing payment_id")
		return
	}

	status := outbound.Status(req.Status)
	if status != outbound.StatusCompleted && status != outbound.StatusRejected {
		writeError(w, http.StatusBadRequest, "status must be completed or rejected")
		return
	}

	_, err := h.svc.UpdateStatusFromWebhook(r.Context(), req.PaymentID, status)
	if err != nil {
		if errors.Is(err, outbound.ErrNotFound) {
			writeError(w, http.StatusNotFound, "payment not found")
			return
		}
		if errors.Is(err, outbound.ErrInvalidStatus) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		h.log.ErrorContext(r.Context(), "webhook payment status", "err", err)
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// getPayment handles GET /payments/{id}.
func (h *Handler) getPayment(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "missing payment id")
		return
	}

	payment, err := h.svc.Get(r.Context(), id)
	if err != nil {
		if errors.Is(err, outbound.ErrNotFound) {
			writeError(w, http.StatusNotFound, "payment not found")
			return
		}
		h.log.ErrorContext(r.Context(), "get payment", "payment_id", id, "err", err)
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	writeJSON(w, http.StatusOK, payment)
}

// listPayments handles GET /payments.
func (h *Handler) listPayments(w http.ResponseWriter, r *http.Request) {
	payments, err := h.svc.List(r.Context())
	if err != nil {
		h.log.ErrorContext(r.Context(), "list payments", "err", err)
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	if payments == nil {
		payments = []outbound.Payment{}
	}

	writeJSON(w, http.StatusOK, payments)
}

// writeJSON serialises v and writes it to w with the given status code.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("write json response", "err", err)
	}
}

// writeError writes a JSON error response.
func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
