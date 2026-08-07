package main

import (
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"
)

func main() {

	r := chi.NewRouter()

	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"status":"ok"}`))
	})

	log.Println("Ingestor starting on :8081")

	err := http.ListenAndServe(":8081", r)
	if err != nil {
		log.Fatal(err)
	}
}