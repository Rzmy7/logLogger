package main

import (
	"net/http"

	_ "github.com/Rzmy7/logLogger/docs/swagger"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	httpSwagger "github.com/swaggo/http-swagger/v2"
)

// NewRouter configures the Chi router and endpoints for ingestor.
func NewRouter(h *Handler) http.Handler {
	r := chi.NewRouter()

	// Middlewares
	r.Use(middleware.RequestID)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	// Swagger UI
	r.Get("/swagger/*", httpSwagger.WrapHandler)

	// Routes
	r.Get("/health", h.HealthCheck)

	r.Route("/api/v1", func(r chi.Router) {
		r.Post("/logs", h.IngestLogs)
	})

	return r
}
