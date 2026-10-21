// Command provision creates the SQS queues, the DLQ redrive policy and the SNS
// events topic. It is idempotent and runs as a one-shot service in Compose.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/devfertonani/wallet-service/internal/config"
	"github.com/devfertonani/wallet-service/internal/platform/awsclient"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "provision:", err)
		os.Exit(1)
	}
	fmt.Println("provision: ok")
}

func run(ctx context.Context) error {
	cfg, err := config.Read()
	if err != nil {
		return err
	}
	ac, err := awsclient.NewAWSConfig(cfg)
	if err != nil {
		return err
	}
	sq, sn := awsclient.NewSQS(ac), awsclient.NewSNS(ac)

	// The emulator may still be booting; retry for a while before failing.
	var last error
	for i := 0; i < 30; i++ {
		if last = awsclient.Provision(ctx, sq, sn, cfg); last == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return last
}
