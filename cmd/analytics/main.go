package main

import (
	"log"
	"os"

	"github.com/Rzmy7/logLogger/internal/config"
	"github.com/Rzmy7/logLogger/internal/elastic"
	"github.com/Rzmy7/logLogger/internal/redis"
	"github.com/Rzmy7/logLogger/internal/retention"
)

func main() {
	// 1. Load Configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// 2. Initialize Elasticsearch Client
	esClient, err := elastic.NewClient(cfg.ElasticsearchURL)
	if err != nil {
		log.Fatalf("Failed to initialize Elasticsearch client: %v", err)
	}

	// 3. Initialize Redis Metrics Reader
	redisClient, err := redis.NewClient(cfg.RedisURL)
	if err != nil {
		log.Fatalf("Failed to initialize Redis client: %v", err)
	}
	defer func() {
		if err := redisClient.Close(); err != nil {
			log.Printf("Error closing Redis client: %v", err)
		}
	}()

	// 4. Initialize Retention Manager for Admin APIs
	retentionManager := retention.NewManager(esClient)

	// 5. Initialize Handlers and Router
	h := NewHandler(redisClient, esClient, retentionManager)
	r := NewRouter(h)

	// 6. Initialize & Start HTTP Server
	port := os.Getenv("ANALYTICS_PORT")
	if port == "" {
		port = "8082"
	}
	if port[0] != ':' {
		port = ":" + port
	}

	srv := NewServer(port, r)
	if err := srv.Start(); err != nil {
		log.Fatalf("Analytics server error: %v", err)
	}
}
