package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"time"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8081"
	}

	// Health check endpoint
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "OK")
	})

	// Main endpoint untuk testing
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if port == "8082" {
			http.Error(w, "Simulated Error", http.StatusInternalServerError)
			return
		}
		
		log.Printf("Backend %s: %s %s", port, r.Method, r.URL.Path)
		time.Sleep(30 * time.Millisecond) // simulasi proses
		fmt.Fprintf(w, "Response dari backend %s pada %s", port, time.Now().Format(time.RFC3339))
	})

	// Endpoint /user contoh
	http.HandleFunc("/user", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"id":123, "name":"Mahasiswa", "backend":"%s"}`, port)
	})

	log.Printf("Backend dummy %s berjalan di :%s", port, port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
