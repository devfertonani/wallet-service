// Package health exposes the public liveness and readiness probes.
package health

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/devfertonani/wallet-service/internal/platform/awsclient"
	"github.com/devfertonani/wallet-service/internal/platform/httpserver"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
)

type Checker struct {
	pool     *pgxpool.Pool
	sqs      *sqs.Client
	queues   *awsclient.Queues
	draining *httpserver.Draining
}

func NewChecker(pool *pgxpool.Pool, sq *sqs.Client, q *awsclient.Queues, d *httpserver.Draining) *Checker {
	return &Checker{pool: pool, sqs: sq, queues: q, draining: d}
}

type report struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks,omitempty"`
}

func (c *Checker) Live() httpserver.Route {
	return httpserver.Route{Pattern: "GET /health/live", Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		write(w, http.StatusOK, report{Status: "UP"})
	})}
}

// Ready checks Postgres and SQS in parallel, each with its own deadline, so a
// dependency that hangs does not make the other one look down.
func (c *Checker) Ready() httpserver.Route {
	return httpserver.Route{Pattern: "GET /health/ready", Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		checks := map[string]func(context.Context) error{
			"postgres": c.pool.Ping,
			"sqs": func(ctx context.Context) error {
				_, err := c.sqs.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
					QueueUrl:       aws.String(c.queues.WagerURL),
					AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
				})
				return err
			},
		}
		results := make(map[string]string, len(checks)+1)
		var mu sync.Mutex
		var wg sync.WaitGroup
		for name, check := range checks {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
				defer cancel()
				status := "UP"
				if err := check(ctx); err != nil {
					status = "DOWN"
				}
				mu.Lock()
				results[name] = status
				mu.Unlock()
			}()
		}
		wg.Wait()
		ok := results["postgres"] == "UP" && results["sqs"] == "UP"
		if c.draining.Active() {
			results["process"] = "DRAINING"
			ok = false
		}
		if ok {
			write(w, http.StatusOK, report{Status: "UP", Checks: results})
			return
		}
		write(w, http.StatusServiceUnavailable, report{Status: "DOWN", Checks: results})
	})}
}

func write(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

var Module = fx.Module("health",
	fx.Provide(NewChecker),
	fx.Provide(
		httpserver.AsRoute(func(c *Checker) httpserver.Route { return c.Live() }),
		httpserver.AsRoute(func(c *Checker) httpserver.Route { return c.Ready() }),
	),
)
