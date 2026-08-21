package main

import (
	"log"

	"github.com/Rzmy7/logLogger/internal/config"
	"github.com/Rzmy7/logLogger/internal/kafka"
)

func main() {
	// 1. Load Configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// 2. Initialize Kafka Producer
	producer, err := kafka.NewProducer(cfg.KafkaBrokers)
	if err != nil {
		log.Fatalf("Failed to initialize Kafka producer: %v", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Printf("Failed to close Kafka producer: %v", err)
		}
	}()

	// 3. Initialize Handlers
	h := NewHandler(producer)

	// 4. Initialize Router
	r := NewRouter(h)

	// 5. Initialize & Start HTTP Server
	srv := NewServer(":"+cfg.HTTPPort, r)
	if err := srv.Start(); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}