package httpapi

import (
	"encoding/base64"
	"net/http"
	"strconv"
	"time"

	"github.com/devfertonani/wallet-service/internal/app/wagering"
	"github.com/devfertonani/wallet-service/internal/domain/events"
	"github.com/devfertonani/wallet-service/internal/domain/failure"
	"github.com/devfertonani/wallet-service/internal/domain/money"
	"github.com/devfertonani/wallet-service/internal/domain/wager"
	"github.com/devfertonani/wallet-service/internal/domain/wallet"
	"github.com/devfertonani/wallet-service/internal/platform/auth"
	"github.com/google/uuid"
)

// ---- wallets ----

type openWalletRequest struct {
	PlayerID       string     `json:"playerId"`
	InitialBalance *money.DTO `json:"initialBalance"`
}

type walletResponse struct {
	ID        uuid.UUID `json:"id"`
	PlayerID  uuid.UUID `json:"playerId"`
	Balance   money.DTO `json:"balance"`
	Version   int64     `json:"version"`
	CreatedAt string    `json:"createdAt"`
	UpdatedAt string    `json:"updatedAt"`
}

func walletBody(w wallet.Wallet) walletResponse {
	return walletResponse{ID: w.ID(), PlayerID: w.PlayerID(), Balance: w.Balance().DTO(), Version: w.Version(),
		CreatedAt: ts(w.CreatedAt()), UpdatedAt: ts(w.UpdatedAt())}
}

func ts(t time.Time) string { return t.UTC().Format(events.TimeFormat) }

func (a *API) openWallet(_ http.ResponseWriter, r *http.Request) (int, any, error) {
	var req openWalletRequest
	if err := decode(r, &req); err != nil {
		return 0, nil, err
	}
	player, err := uuid.Parse(req.PlayerID)
	if err != nil || player == uuid.Nil {
		return 0, nil, badRequest(failure.InvalidRequest, "playerId must be a UUID")
	}
	initial, err := moneyInput(req.InitialBalance, "initialBalance")
	if err != nil {
		return 0, nil, err
	}
	w, err := a.svc.OpenWallet(r.Context(), player, initial)
	if err != nil {
		return 0, nil, err
	}
	return http.StatusCreated, walletBody(w), nil
}

func (a *API) getWallet(_ http.ResponseWriter, r *http.Request) (int, any, error) {
	id, err := pathUUID(r, "walletId")
	if err != nil {
		return 0, nil, err
	}
	w, err := a.svc.GetWallet(r.Context(), id)
	if err != nil {
		return 0, nil, err
	}
	return http.StatusOK, walletBody(w), nil
}

type ledgerEntryResponse struct {
	ID            uuid.UUID `json:"id"`
	TransactionID uuid.UUID `json:"transactionId"`
	Direction     string    `json:"direction"`
	Money         money.DTO `json:"money"`
	BalanceBefore money.DTO `json:"balanceBefore"`
	BalanceAfter  money.DTO `json:"balanceAfter"`
	WalletVersion int64     `json:"walletVersion"`
	CreatedAt     string    `json:"createdAt"`
}

type ledgerResponse struct {
	WalletID   uuid.UUID             `json:"walletId"`
	Items      []ledgerEntryResponse `json:"items"`
	NextCursor string                `json:"nextCursor,omitempty"`
}

const (
	defaultLedgerLimit = 50
	maxLedgerLimit     = 200
)

// The cursor is the last ledger sequence number of the page, base64url
// encoded: opaque for the client, stable because seq only grows.
func encodeCursor(seq int64) string {
	return base64.RawURLEncoding.EncodeToString([]byte("seq:" + strconv.FormatInt(seq, 10)))
}

func decodeCursor(s string) (int64, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil || len(b) < 5 || string(b[:4]) != "seq:" {
		return 0, badRequest(failure.InvalidRequest, "invalid cursor")
	}
	seq, err := strconv.ParseInt(string(b[4:]), 10, 64)
	if err != nil || seq < 0 {
		return 0, badRequest(failure.InvalidRequest, "invalid cursor")
	}
	return seq, nil
}

func (a *API) getLedger(_ http.ResponseWriter, r *http.Request) (int, any, error) {
	id, err := pathUUID(r, "walletId")
	if err != nil {
		return 0, nil, err
	}
	limit := defaultLedgerLimit
	if v := r.URL.Query().Get("limit"); v != "" {
		if limit, err = strconv.Atoi(v); err != nil || limit < 1 || limit > maxLedgerLimit {
			return 0, nil, badRequest(failure.InvalidRequest, "limit must be between 1 and %d", maxLedgerLimit)
		}
	}
	var after int64
	if c := r.URL.Query().Get("cursor"); c != "" {
		if after, err = decodeCursor(c); err != nil {
			return 0, nil, err
		}
	}
	// One extra row tells whether there is a next page.
	rows, err := a.svc.LedgerPage(r.Context(), id, after, limit+1)
	if err != nil {
		return 0, nil, err
	}
	resp := ledgerResponse{WalletID: id, Items: make([]ledgerEntryResponse, 0, min(len(rows), limit))}
	for i, row := range rows {
		if i == limit {
			resp.NextCursor = encodeCursor(rows[i-1].Seq)
			break
		}
		e := row.Entry
		resp.Items = append(resp.Items, ledgerEntryResponse{
			ID: e.ID(), TransactionID: e.TransactionID(), Direction: string(e.Direction()), Money: e.Amount().DTO(),
			BalanceBefore: e.BalanceBefore().DTO(), BalanceAfter: e.BalanceAfter().DTO(), WalletVersion: e.WalletVersion(),
			CreatedAt: ts(e.CreatedAt()),
		})
	}
	return http.StatusOK, resp, nil
}

type reconciliationResponse struct {
	WalletID          uuid.UUID `json:"walletId"`
	StoredBalance     money.DTO `json:"storedBalance"`
	CalculatedBalance money.DTO `json:"calculatedBalance"`
	Difference        money.DTO `json:"difference"`
	Consistent        bool      `json:"consistent"`
	CheckedEntries    int64     `json:"checkedEntries"`
}

func (a *API) reconcile(_ http.ResponseWriter, r *http.Request) (int, any, error) {
	id, err := pathUUID(r, "walletId")
	if err != nil {
		return 0, nil, err
	}
	rec, err := a.svc.Reconcile(r.Context(), id)
	if err != nil {
		return 0, nil, err
	}
	return http.StatusOK, reconciliationResponse{
		WalletID: rec.WalletID, StoredBalance: rec.StoredBalance.DTO(), CalculatedBalance: rec.CalculatedBalance.DTO(),
		Difference: rec.Difference.DTO(), Consistent: rec.Consistent, CheckedEntries: rec.CheckedEntries,
	}, nil
}

// ---- wager transactions ----

type submitRequest struct {
	ProviderID                     string     `json:"providerId"`
	ExternalTransactionID          string     `json:"externalTransactionId"`
	PlayerID                       string     `json:"playerId"`
	WalletID                       string     `json:"walletId"`
	RoundID                        string     `json:"roundId"`
	GameID                         string     `json:"gameId"`
	Kind                           string     `json:"kind"`
	Money                          *money.DTO `json:"money"`
	ReferenceExternalTransactionID string     `json:"referenceExternalTransactionId,omitempty"`
}

type submitResponse struct {
	TransactionID    uuid.UUID  `json:"transactionId"`
	Status           string     `json:"status"`
	Balance          *money.DTO `json:"balance,omitempty"`
	FailureCode      string     `json:"failureCode,omitempty"`
	IdempotentReplay bool       `json:"idempotentReplay"`
}

// statusCode maps a persisted outcome onto HTTP. A replay answers with the
// same code as the original.
func statusCode(s wager.Status) int {
	switch s {
	case wager.Processed:
		return http.StatusOK
	case wager.PendingReference:
		return http.StatusAccepted
	case wager.Rejected:
		return http.StatusUnprocessableEntity
	}
	return http.StatusInternalServerError // FAILED: permanent infrastructure failure, recorded for audit
}

func (a *API) submit(_ http.ResponseWriter, r *http.Request) (int, any, error) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		return 0, nil, badRequest(failure.InvalidRequest, "Idempotency-Key header is required")
	}
	var req submitRequest
	if err := decode(r, &req); err != nil {
		return 0, nil, err
	}
	p := principal(r.Context())
	// The token decides the provider; the body must agree with it.
	if req.ProviderID != p.ProviderID || p.ProviderID == "" {
		return 0, nil, forbidden("providerId does not match the authenticated provider")
	}
	if req.Money == nil {
		return 0, nil, badRequest(failure.InvalidRequest, "money is required")
	}
	cmd, err := wager.NewCommand(wager.CommandInput{
		ProviderID: req.ProviderID, ExternalTransactionID: req.ExternalTransactionID, IdempotencyKey: key,
		PlayerID: req.PlayerID, WalletID: req.WalletID, RoundID: req.RoundID, GameID: req.GameID,
		Kind: req.Kind, Amount: req.Money.Amount, Currency: req.Money.Currency,
		ReferenceExternalTransactionID: req.ReferenceExternalTransactionID,
	})
	if err != nil {
		return 0, nil, err
	}
	corr := correlationID(r.Context())
	res, err := a.svc.Submit(r.Context(), cmd, wagering.Source{Channel: wagering.ChannelHTTP, CorrelationID: corr, CausationID: corr})
	if err != nil {
		return 0, nil, err
	}
	s := res.Transaction
	body := submitResponse{TransactionID: s.ID, Status: string(s.Status), FailureCode: string(s.FailureCode), IdempotentReplay: res.Replay}
	if s.ResultBalance != nil {
		d := s.ResultBalance.DTO()
		body.Balance = &d
	}
	a.log.InfoContext(r.Context(), "wager operation", "transactionId", s.ID, "walletId", s.WalletID, "providerId", s.ProviderID,
		"kind", s.Kind, "status", s.Status, "failureCode", s.FailureCode, "replay", res.Replay, "correlationId", corr)
	return statusCode(s.Status), body, nil
}

type transactionResponse struct {
	TransactionID                  uuid.UUID  `json:"transactionId"`
	ProviderID                     string     `json:"providerId,omitempty"`
	ExternalTransactionID          string     `json:"externalTransactionId,omitempty"`
	WalletID                       uuid.UUID  `json:"walletId"`
	PlayerID                       uuid.UUID  `json:"playerId"`
	RoundID                        string     `json:"roundId,omitempty"`
	GameID                         string     `json:"gameId,omitempty"`
	Kind                           string     `json:"kind"`
	Money                          money.DTO  `json:"money"`
	Status                         string     `json:"status"`
	FailureCode                    string     `json:"failureCode,omitempty"`
	Balance                        *money.DTO `json:"balance,omitempty"`
	ReferenceExternalTransactionID string     `json:"referenceExternalTransactionId,omitempty"`
	ReferenceTransactionID         *uuid.UUID `json:"referenceTransactionId,omitempty"`
	Attempts                       int        `json:"attempts"`
	NextAttemptAt                  string     `json:"nextAttemptAt,omitempty"`
	ExpiresAt                      string     `json:"expiresAt,omitempty"`
	CreatedAt                      string     `json:"createdAt"`
	UpdatedAt                      string     `json:"updatedAt"`
}

func transactionBody(s wager.Snapshot) transactionResponse {
	t := transactionResponse{
		TransactionID: s.ID, ProviderID: s.ProviderID, ExternalTransactionID: s.ExternalTransactionID,
		WalletID: s.WalletID, PlayerID: s.PlayerID, RoundID: s.RoundID, GameID: s.GameID, Kind: string(s.Kind),
		Money: s.Money.DTO(), Status: string(s.Status), FailureCode: string(s.FailureCode),
		ReferenceExternalTransactionID: s.ReferenceExternalTransactionID, Attempts: s.Attempts,
		CreatedAt: ts(s.CreatedAt), UpdatedAt: ts(s.UpdatedAt),
	}
	if s.ResultBalance != nil {
		d := s.ResultBalance.DTO()
		t.Balance = &d
	}
	if s.ReferenceTransactionID != uuid.Nil {
		id := s.ReferenceTransactionID
		t.ReferenceTransactionID = &id
	}
	if s.NextAttemptAt != nil {
		t.NextAttemptAt = ts(*s.NextAttemptAt)
	}
	if s.ExpiresAt != nil {
		t.ExpiresAt = ts(*s.ExpiresAt)
	}
	return t
}

func (a *API) getTransaction(_ http.ResponseWriter, r *http.Request) (int, any, error) {
	id, err := pathUUID(r, "transactionId")
	if err != nil {
		return 0, nil, err
	}
	s, err := a.svc.GetTransaction(r.Context(), id)
	if err != nil {
		return 0, nil, err
	}
	// A provider sees only its own transactions; another provider's (or an
	// internal OPENING) is reported as not found, without confirming it exists.
	if p := principal(r.Context()); !p.Has(auth.RoleInternal) && (p.ProviderID == "" || s.ProviderID != p.ProviderID) {
		return 0, nil, notFoundErr("transaction not found")
	}
	return http.StatusOK, transactionBody(s), nil
}

func (a *API) getByExternalID(_ http.ResponseWriter, r *http.Request) (int, any, error) {
	provider := r.PathValue("providerId")
	if p := principal(r.Context()); p.ProviderID == "" || provider != p.ProviderID {
		return 0, nil, forbidden("providerId does not match the authenticated provider")
	}
	s, err := a.svc.GetByExternalID(r.Context(), provider, r.PathValue("externalTransactionId"))
	if err != nil {
		return 0, nil, err
	}
	return http.StatusOK, transactionBody(s), nil
}
