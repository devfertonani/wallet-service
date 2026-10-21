// Package httpapi exposes the wallet contract over HTTP. Handlers authenticate
// and authorize before touching any financial data, translate the request into
// the same command the SQS consumer builds, and map results and errors onto
// the documented status codes.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/devfertonani/wallet-service/internal/app/wagering"
	"github.com/devfertonani/wallet-service/internal/config"
	"github.com/devfertonani/wallet-service/internal/domain/failure"
	"github.com/devfertonani/wallet-service/internal/domain/money"
	"github.com/devfertonani/wallet-service/internal/platform/auth"
	"github.com/devfertonani/wallet-service/internal/platform/httpserver"
	"github.com/devfertonani/wallet-service/internal/platform/metrics"
	"github.com/google/uuid"
	"go.uber.org/fx"
)

// maxBody bounds request bodies; a wager operation is a few hundred bytes.
const maxBody = 64 << 10

type API struct {
	timeout  time.Duration
	svc      *wagering.Service
	verifier *auth.Verifier
	metrics  *metrics.Metrics
	log      *slog.Logger
}

func New(cfg config.Config, svc *wagering.Service, v *auth.Verifier, m *metrics.Metrics, log *slog.Logger) *API {
	return &API{timeout: cfg.RequestTimeout, svc: svc, verifier: v, metrics: m, log: log.With("component", "http")}
}

// Routes lists every business route with the roles allowed to call it.
func (a *API) Routes() []httpserver.Route {
	r := func(pattern string, h handler, roles ...string) httpserver.Route {
		return httpserver.Route{Pattern: pattern, Handler: a.wrap(pattern, roles, h)}
	}
	return []httpserver.Route{
		r("POST /wallets", a.openWallet, auth.RoleInternal),
		r("GET /wallets/{walletId}", a.getWallet, auth.RoleInternal),
		r("GET /wallets/{walletId}/ledger", a.getLedger, auth.RoleInternal),
		r("POST /wallets/{walletId}/reconciliation", a.reconcile, auth.RoleInternal),
		r("POST /wagering/transactions", a.submit, auth.RoleProvider),
		r("GET /wagering/transactions/{transactionId}", a.getTransaction, auth.RoleProvider, auth.RoleInternal),
		r("GET /providers/{providerId}/wagering/transactions/{externalTransactionId}", a.getByExternalID, auth.RoleProvider),
	}
}

type principalKey struct{}

func principal(ctx context.Context) auth.Principal {
	p, _ := ctx.Value(principalKey{}).(auth.Principal)
	return p
}

type correlationKey struct{}

func correlationID(ctx context.Context) string {
	id, _ := ctx.Value(correlationKey{}).(string)
	return id
}

// handler returns the status and body, or an error mapped by writeError.
type handler func(w http.ResponseWriter, r *http.Request) (int, any, error)

type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (s *statusRecorder) WriteHeader(code int) { s.code = code; s.ResponseWriter.WriteHeader(code) }

// wrap applies, in order: correlation id, authentication, role check, the
// handler, error mapping, metrics and the access log. Authorization happens
// before the handler runs, so a refused request never opens a transaction.
func (a *API) wrap(route string, roles []string, h handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		corr := r.Header.Get("X-Correlation-Id")
		if corr == "" || len(corr) > 128 {
			corr = uuid.NewString()
		}
		w.Header().Set("X-Correlation-Id", corr)
		ctx := context.WithValue(r.Context(), correlationKey{}, corr)
		rec := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
		var p auth.Principal

		defer func() {
			if v := recover(); v != nil {
				a.log.ErrorContext(ctx, "panic in handler", "route", route, "panic", fmt.Sprint(v), "correlationId", corr)
				writeJSON(rec, http.StatusInternalServerError, errorBody("INTERNAL", "internal error"))
			}
			a.metrics.HTTPRequests.WithLabelValues(route, fmt.Sprint(rec.code)).Inc()
			a.metrics.HTTPDuration.WithLabelValues(route).Observe(time.Since(start).Seconds())
			a.log.InfoContext(ctx, "http request", "route", route, "status", rec.code,
				"durationMs", time.Since(start).Milliseconds(), "correlationId", corr, "clientId", p.ClientID, "providerId", p.ProviderID)
		}()

		scheme, raw, ok := strings.Cut(r.Header.Get("Authorization"), " ")
		if !ok || !strings.EqualFold(scheme, "Bearer") || raw == "" {
			rec.Header().Set("WWW-Authenticate", `Bearer`)
			writeJSON(rec, http.StatusUnauthorized, errorBody("UNAUTHENTICATED", "missing bearer token"))
			return
		}
		var err error
		if p, err = a.verifier.Verify(ctx, raw); err != nil {
			rec.Header().Set("WWW-Authenticate", `Bearer error="invalid_token"`)
			writeJSON(rec, http.StatusUnauthorized, errorBody("UNAUTHENTICATED", "invalid or expired token"))
			return
		}
		allowed := false
		for _, role := range roles {
			allowed = allowed || p.Has(role)
		}
		if !allowed {
			writeJSON(rec, http.StatusForbidden, errorBody("FORBIDDEN", "the token has no role allowed on this route"))
			return
		}
		// Every request has a deadline, so a database that stops answering
		// turns into 503 instead of a connection held open indefinitely.
		ctx, cancel := context.WithTimeout(context.WithValue(ctx, principalKey{}, p), a.timeout)
		defer cancel()
		r = r.WithContext(ctx)
		r.Body = http.MaxBytesReader(rec, r.Body, maxBody)

		code, body, err := h(rec, r)
		if err != nil {
			a.writeError(rec, r, err)
			return
		}
		writeJSON(rec, code, body)
	})
}

// apiError is a contract error with its status.
type apiError struct {
	status  int
	code    string
	message string
}

func (e *apiError) Error() string { return e.code + ": " + e.message }

func badRequest(code failure.Code, format string, args ...any) error {
	return &apiError{http.StatusBadRequest, string(code), fmt.Sprintf(format, args...)}
}

func forbidden(msg string) error   { return &apiError{http.StatusForbidden, "FORBIDDEN", msg} }
func notFoundErr(msg string) error { return &apiError{http.StatusNotFound, "NOT_FOUND", msg} }

func (a *API) writeError(w http.ResponseWriter, r *http.Request, err error) {
	var ae *apiError
	var fe *failure.Error
	switch {
	case errors.As(err, &ae):
		writeJSON(w, ae.status, errorBody(ae.code, ae.message))
	case errors.As(err, &fe):
		status := http.StatusBadRequest
		if fe.Code == failure.WalletNotFound {
			status = http.StatusNotFound
		}
		writeJSON(w, status, errorBody(string(fe.Code), fe.Message))
	case errors.Is(err, wagering.ErrNotFound):
		writeJSON(w, http.StatusNotFound, errorBody("NOT_FOUND", "resource not found"))
	case errors.Is(err, wagering.ErrIdempotencyKeyReused):
		writeJSON(w, http.StatusConflict, errorBody("IDEMPOTENCY_KEY_REUSED", "the idempotency key was used with a different payload"))
	case errors.Is(err, wagering.ErrDuplicateExternal):
		writeJSON(w, http.StatusConflict, errorBody("DUPLICATE_EXTERNAL_TRANSACTION", "the external transaction was already received with another idempotency key"))
	case errors.Is(err, wagering.ErrWalletExists):
		writeJSON(w, http.StatusConflict, errorBody("WALLET_ALREADY_EXISTS", "the player already has a wallet in this currency"))
	case errors.Is(err, wagering.ErrTransient):
		a.log.WarnContext(r.Context(), "transient failure", "error", err, "correlationId", correlationID(r.Context()))
		w.Header().Set("Retry-After", "1")
		writeJSON(w, http.StatusServiceUnavailable, errorBody("TEMPORARILY_UNAVAILABLE", "temporary failure, retry with the same Idempotency-Key"))
	default:
		a.log.ErrorContext(r.Context(), "unexpected error", "error", err, "correlationId", correlationID(r.Context()))
		writeJSON(w, http.StatusInternalServerError, errorBody("INTERNAL", "internal error"))
	}
}

type errorResponse struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func errorBody(code, msg string) errorResponse {
	var e errorResponse
	e.Error.Code, e.Error.Message = code, msg
	return e
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

// decode reads exactly one JSON object, rejecting unknown fields and trailing
// data. Amounts are strings in the DTOs, so a JSON number fails here and never
// passes through float64.
func decode(r *http.Request, dst any) error {
	if ct := r.Header.Get("Content-Type"); ct != "" && !strings.HasPrefix(ct, "application/json") {
		return badRequest(failure.InvalidRequest, "Content-Type must be application/json")
	}
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			return badRequest(failure.InvalidRequest, "request body too large")
		}
		var te *json.UnmarshalTypeError
		if errors.As(err, &te) && strings.Contains(te.Field, "amount") {
			return badRequest(failure.InvalidAmount, "amount must be a decimal string such as \"25.00\"")
		}
		return badRequest(failure.InvalidRequest, "invalid JSON body: %v", err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return badRequest(failure.InvalidRequest, "unexpected data after the JSON object")
	}
	return nil
}

func pathUUID(r *http.Request, name string) (uuid.UUID, error) {
	id, err := uuid.Parse(r.PathValue(name))
	if err != nil {
		// An id that cannot exist is reported like one that does not.
		return uuid.Nil, notFoundErr(name + " not found")
	}
	return id, nil
}

// moneyInput parses an external amount: canonical decimal string, supported
// currency, never negative.
func moneyInput(d *money.DTO, field string) (money.Money, error) {
	if d == nil {
		return money.Money{}, badRequest(failure.InvalidRequest, "%s is required", field)
	}
	m, err := money.ParseNonNegative(d.Amount, d.Currency)
	switch {
	case errors.Is(err, money.ErrInvalidCurrency):
		return money.Money{}, badRequest(failure.InvalidCurrency, "unsupported currency %q", d.Currency)
	case err != nil:
		return money.Money{}, badRequest(failure.InvalidAmount, "%s: %v", field, err)
	}
	return m, nil
}

func NewRoutes(a *API) []httpserver.Route { return a.Routes() }

var Module = fx.Module("httpapi",
	fx.Provide(New),
	fx.Provide(fx.Annotate(NewRoutes, fx.ResultTags(`group:"routes,flatten"`))),
)
