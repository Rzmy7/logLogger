package main

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

// NewRouter configures the Chi router and endpoints for Analytics API.
func NewRouter(h *Handler) http.Handler {
	r := chi.NewRouter()

	// Middlewares
	r.Use(middleware.RequestID)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	// Routes
	r.Get("/health", h.HealthCheck)
	r.Get("/metrics/live", h.LiveMetrics)
	r.Get("/metrics/top-errors", h.TopErrors)
	r.Get("/metrics/top-services", h.TopServices)
	r.Get("/search", h.Search)

	return r
}
