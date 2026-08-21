package main

import (
	"log"

	"github.com/Rzmy7/logLogger/internal/config"
)

func main() {
	// 1. Load Configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// 2. Initialize Handlers
	h := NewHandler()

	// 3. Initialize Router
	r := NewRouter(h)

	// 4. Initialize & Start HTTP Server
	srv := NewServer(":"+cfg.HTTPPort, r)
	if err := srv.Start(); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}