package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	cfg, err := ParseFlags()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Configuration error: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	runner := NewRunner(cfg)
	summary, err := runner.Run(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Load generator error: %v\n", err)
		os.Exit(1)
	}

	summary.PrintReport()
}
