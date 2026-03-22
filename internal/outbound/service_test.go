package outbound_test

import (
	"context"
	"testing"

	"github.com/2beens/simple-go-service/internal/outbound"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestService_HandleOutboundPayment_DuplicateIdempotency(t *testing.T) {
	ctx := t.Context()
	ctrl := gomock.NewController(t)
	idempotency := NewMockidempotencyStore(ctrl)

	idempotency.EXPECT().
		KeyExists(gomock.Any(), "key-1").
		Return(true, nil)

	svc := outbound.NewService(
		NewMockrepository(ctrl),
		idempotency,
		NewMockform3Creator(ctrl),
		NewMockstatusPublisher(ctrl),
	)

	_, err := svc.HandleOutboundPayment(ctx, "key-1", outbound.CreatePayload{
		Amount:       100,
		Currency:     "EUR",
		DebtorName:   "Debtor",
		CreditorName: "Creditor",
	})
	require.Error(t, err)
	require.ErrorIs(t, err, outbound.ErrDuplicateIdempotency)
}

func TestService_HandleOutboundPayment_ValidationError(t *testing.T) {
	ctx := t.Context()
	ctrl := gomock.NewController(t)
	idempotency := NewMockidempotencyStore(ctrl)

	idempotency.EXPECT().
		KeyExists(gomock.Any(), "key-1").
		Return(false, nil)

	svc := outbound.NewService(
		NewMockrepository(ctrl),
		idempotency,
		NewMockform3Creator(ctrl),
		NewMockstatusPublisher(ctrl),
	)

	_, err := svc.HandleOutboundPayment(ctx, "key-1", outbound.CreatePayload{
		Amount:       0,
		Currency:     "EUR",
		DebtorName:   "Debtor",
		CreditorName: "Creditor",
	})
	require.Error(t, err)
	require.ErrorIs(t, err, outbound.ErrInvalidAmount)
}

func TestService_HandleOutboundPayment_HappyPath(t *testing.T) {
	ctx := t.Context()
	ctrl := gomock.NewController(t)
	repo := NewMockrepository(ctrl)
	idempotency := NewMockidempotencyStore(ctrl)
	form3 := NewMockform3Creator(ctrl)

	idempotency.EXPECT().
		KeyExists(gomock.Any(), "idempotency-happy-1").
		Return(false, nil)
	form3.EXPECT().
		CreatePayment(gomock.Any(), gomock.AssignableToTypeOf(outbound.Payment{})).
		DoAndReturn(func(_ context.Context, p outbound.Payment) error {
			assert.NotEmpty(t, p.ID, "Form3 CreatePayment: expected non-empty payment ID")
			assert.Equal(t, int64(500), p.Amount)
			assert.Equal(t, "USD", p.Currency)
			assert.Equal(t, "Debtor", p.DebtorName)
			assert.Equal(t, "Creditor", p.CreditorName)
			assert.Equal(t, "idempotency-happy-1", p.IdempotencyKey)
			assert.Equal(t, outbound.StatusCreated, p.Status)
			return nil
		})
	repo.EXPECT().
		Save(gomock.Any(), gomock.AssignableToTypeOf(outbound.Payment{})).
		DoAndReturn(func(_ context.Context, p outbound.Payment) error {
			assert.NotEmpty(t, p.ID, "Save: expected non-empty payment ID")
			assert.Equal(t, int64(500), p.Amount)
			assert.Equal(t, "USD", p.Currency)
			assert.Equal(t, "Debtor", p.DebtorName)
			assert.Equal(t, "Creditor", p.CreditorName)
			assert.Equal(t, "idempotency-happy-1", p.IdempotencyKey)
			assert.Equal(t, outbound.StatusCreated, p.Status)
			return nil
		})
	idempotency.EXPECT().
		SetKey(gomock.Any(), "idempotency-happy-1").
		Return(nil)

	svc := outbound.NewService(repo, idempotency, form3, NewMockstatusPublisher(ctrl))

	payment, err := svc.HandleOutboundPayment(ctx, "idempotency-happy-1", outbound.CreatePayload{
		Amount:       500,
		Currency:     "USD",
		DebtorName:   "Debtor",
		CreditorName: "Creditor",
	})
	require.NoError(t, err)

	require.NotEmpty(t, payment.ID, "expected non-empty payment ID")
	assert.Equal(t, outbound.StatusCreated, payment.Status)
}

func TestService_UpdateStatusFromWebhook_InvalidStatus(t *testing.T) {
	ctx := t.Context()
	ctrl := gomock.NewController(t)

	svc := outbound.NewService(
		NewMockrepository(ctrl),
		NewMockidempotencyStore(ctrl),
		NewMockform3Creator(ctrl),
		NewMockstatusPublisher(ctrl),
	)

	_, err := svc.UpdateStatusFromWebhook(ctx, "pay-1", outbound.StatusCreated)
	require.Error(t, err)
	require.ErrorIs(t, err, outbound.ErrInvalidStatus)
}

func TestCreatePayload_Validate(t *testing.T) {
	err := (outbound.CreatePayload{
		Amount: 100, Currency: "EUR", DebtorName: "Debtor", CreditorName: "Creditor",
	}).Validate()
	require.NoError(t, err)

	err = (outbound.CreatePayload{
		Amount: 0, Currency: "EUR", DebtorName: "D", CreditorName: "C",
	}).Validate()
	require.ErrorIs(t, err, outbound.ErrInvalidAmount)

	err = (outbound.CreatePayload{
		Amount: 100, Currency: "AB", DebtorName: "D", CreditorName: "C",
	}).Validate()
	require.ErrorIs(t, err, outbound.ErrInvalidCurrency)

	err = (outbound.CreatePayload{
		Amount: 100, Currency: "EUR", DebtorName: "", CreditorName: "Creditor",
	}).Validate()
	require.ErrorIs(t, err, outbound.ErrInvalidDebtorName)

	err = (outbound.CreatePayload{
		Amount: 100, Currency: "EUR", DebtorName: "Debtor", CreditorName: "",
	}).Validate()
	require.ErrorIs(t, err, outbound.ErrInvalidCreditorName)
}
