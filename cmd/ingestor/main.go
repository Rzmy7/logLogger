package main

import (
	"log"

	"github.com/Rzmy7/logLogger/internal/config"
	"github.com/Rzmy7/logLogger/internal/kafka"
)

// @title           Log Ingestor API
// @version         1.0
// @description     High-throughput log ingestion service for logLogger platform.
// @termsOfService  http://swagger.io/terms/

// @contact.name    API Support
// @contact.email   support@loglogger.local

// @license.name    MIT
// @license.url     https://opensource.org/licenses/MIT

// @host      localhost:8081
// @BasePath  /

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