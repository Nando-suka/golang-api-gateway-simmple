package main

import (
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// Backend merepresentasikan satu server backend
type Backend struct {
	URL    *url.URL
	Alive  bool
	Weight int
	mu     sync.RWMutex
	client *http.Client
}

// LoadBalancer mengelola daftar backend dan strategi round-robin
type LoadBalancer struct {
	backends []*Backend
	current  int
	mu       sync.Mutex
}

// NewLoadBalancer membuat load balancer dari daftar URL string
func NewLoadBalancer(urlStrings []string) (*LoadBalancer, error) {
	var backends []*Backend
	for _, urlStr := range urlStrings {
		u, err := url.Parse(urlStr)
		if err != nil {
			return nil, err
		}
		backends = append(backends, &Backend{
			URL:    u,
			Alive:  true,
			Weight: 1,
			client: &http.Client{Timeout: 2 * time.Second},
		})
	}
	return &LoadBalancer{
		backends: backends,
		current:  -1,
	}, nil
}

// NextRoundRobin mengembalikan backend hidup berikutnya secara round-robin
func (lb *LoadBalancer) NextRoundRobin() *Backend {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	total := len(lb.backends)
	if total == 0 {
		return nil
	}
	for i := 0; i < total; i++ {
		lb.current = (lb.current + 1) % total
		backend := lb.backends[lb.current]
		backend.mu.RLock()
		alive := backend.Alive
		backend.mu.RUnlock()
		if alive {
			return backend
		}
	}
	return nil // semua mati
}

// isBackendAlive mengecek kesehatan backend via /health
func (lb *LoadBalancer) isBackendAlive(backend *Backend) bool {
	healthURL := backend.URL.String() + "/health"
	req, err := http.NewRequest("GET", healthURL, nil)
	if err != nil {
		return false
	}
	resp, err := backend.client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNoContent
}

// HealthCheck melakukan pengecekan periodik terhadap semua backend
func (lb *LoadBalancer) HealthCheck(interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		for range ticker.C {
			for _, backend := range lb.backends {
				alive := lb.isBackendAlive(backend)
				backend.mu.Lock()
				backend.Alive = alive
				backend.mu.Unlock()
				status := "up"
				if !alive {
					status = "down"
				}
				log.Printf("[HealthCheck] %s is %s", backend.URL, status)
			}
		}
	}()
}

// UpdateBackends (opsional) untuk mengubah daftar backend secara dinamis
func (lb *LoadBalancer) UpdateBackends(newURLs []string) error {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	var newBackends []*Backend
	for _, urlStr := range newURLs {
		u, err := url.Parse(urlStr)
		if err != nil {
			return err
		}
		newBackends = append(newBackends, &Backend{
			URL:    u,
			Alive:  true,
			Weight: 1,
			client: &http.Client{Timeout: 2 * time.Second},
		})
	}
	lb.backends = newBackends
	lb.current = -1
	return nil
}
