package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"time"

	"github.com/go-redis/redis/v8"
)

// ------------------------------
// Global & Redis Setup
// ------------------------------
var (
	ctx          = context.Background()
	rdb          *redis.Client
	rateLimitSec = 10
	windowSec    = int64(10)
	cacheTTL     = 30 * time.Second
)

type CachedResponse struct {
	Status    int               `json:"status"`
	Header    map[string]string `json:"header"`
	Body      []byte            `json:"body"`
	CreatedAt time.Time         `json:"created_at"`
}

func initRedis() {
	rdb = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})
	if _, err := rdb.Ping(ctx).Result(); err != nil {
		log.Fatal("Gagal konek ke Redis:", err)
	}
	log.Println("✅ Terhubung ke Redis")
}

// ------------------------------
// Helper: ResponseWriter untuk intercept status & body
// ------------------------------
type cacheWriter struct {
	http.ResponseWriter
	body    *bytes.Buffer
	status  int
	headers http.Header
}

func (cw *cacheWriter) WriteHeader(code int) {
	cw.status = code
	cw.ResponseWriter.WriteHeader(code)
}

func (cw *cacheWriter) Write(b []byte) (int, error) {
	cw.body.Write(b)
	return cw.ResponseWriter.Write(b)
}

func (cw *cacheWriter) Header() http.Header {
	h := cw.ResponseWriter.Header()
	for k, v := range h {
		cw.headers[k] = v
	}
	return h
}

type statusWriter struct {
	http.ResponseWriter
	statusCode int
}

func (sw *statusWriter) WriteHeader(code int) {
	sw.statusCode = code
	sw.ResponseWriter.WriteHeader(code)
}

// ------------------------------
// Middleware: Logging
// ------------------------------
func loggingMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, statusCode: http.StatusOK}
		next(sw, r)
		log.Printf("[LOG] %s %s - %d (%v)", r.Method, r.URL.Path, sw.statusCode, time.Since(start))
	}
}

// ------------------------------
// Middleware: Rate Limiting (Sliding Window)
// ------------------------------
func rateLimitMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		apiKey := r.Header.Get("X-API-Key")
		if apiKey == "" {
			http.Error(w, "Missing X-API-Key header", http.StatusUnauthorized)
			return
		}
		now := time.Now().Unix()
		windowStart := now - windowSec
		key := fmt.Sprintf("rate:%s", apiKey)

		pipe := rdb.Pipeline()
		pipe.ZRemRangeByScore(ctx, key, "0", fmt.Sprintf("%d", windowStart))
		pipe.ZAdd(ctx, key, &redis.Z{Score: float64(now), Member: now})
		pipe.ZCard(ctx, key)
		pipe.Expire(ctx, key, time.Duration(windowSec+1)*time.Second)

		cmds, err := pipe.Exec(ctx)
		if err != nil {
			log.Println("Redis error:", err)
			http.Error(w, "Internal Error", http.StatusInternalServerError)
			return
		}
		count := cmds[2].(*redis.IntCmd).Val()
		if count > int64(rateLimitSec) {
			http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
			return
		}
		next(w, r)
	}
}

// ------------------------------
// Middleware: Caching (only GET)
// ------------------------------
func cacheMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			next(w, r)
			return
		}
		apiKey := r.Header.Get("X-API-Key")
		cacheKey := fmt.Sprintf("cache:%s:%s", apiKey, r.URL.String())
		cached, err := rdb.Get(ctx, cacheKey).Result()
		if err == nil {
			var resp CachedResponse
			if json.Unmarshal([]byte(cached), &resp) == nil {
				for k, v := range resp.Header {
					w.Header().Set(k, v)
				}
				w.WriteHeader(resp.Status)
				w.Write(resp.Body)
				log.Printf("[CACHE HIT] %s %s", r.Method, r.URL.Path)
				return
			}
		}
		log.Printf("[CACHE MISS] %s %s", r.Method, r.URL.Path)

		cw := &cacheWriter{
			ResponseWriter: w,
			body:           &bytes.Buffer{},
			headers:        make(http.Header),
			status:         http.StatusOK,
		}
		next(cw, r)

		if cw.status == http.StatusOK {
			cachedResp := CachedResponse{
				Status: cw.status,
				Header: make(map[string]string),
				Body:   cw.body.Bytes(),
			}
			for k, v := range cw.headers {
				if len(v) > 0 {
					cachedResp.Header[k] = v[0]
				}
			}
			data, _ := json.Marshal(cachedResp)
			rdb.Set(ctx, cacheKey, data, cacheTTL)
		}
	}
}

// ------------------------------
// Handler Utama dengan Load Balancer
// ------------------------------
type GatewayHandler struct {
	lb *LoadBalancer
}

func (gh *GatewayHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	backend := gh.lb.NextRoundRobin()
	if backend == nil {
		http.Error(w, "No available backend", http.StatusServiceUnavailable)
		log.Printf("[ERROR] No backend alive for %s %s", r.Method, r.URL.Path)
		return
	}
	proxy := httputil.NewSingleHostReverseProxy(backend.URL)
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("[Proxy Error] %s: %v", backend.URL, err)
		http.Error(w, "Bad Gateway", http.StatusBadGateway)
	}
	log.Printf("[LB] Forward %s %s to %s", r.Method, r.URL.Path, backend.URL)
	proxy.ServeHTTP(w, r)
}

// ------------------------------
// Main
// ------------------------------
func main() {
	initRedis()

	backendURLs := []string{
		"http://localhost:8081",
		"http://localhost:8082",
		"http://localhost:8083",
	}
	lb, err := NewLoadBalancer(backendURLs)
	if err != nil {
		log.Fatal("Gagal buat load balancer:", err)
	}
	lb.HealthCheck(10 * time.Second)

	gw := &GatewayHandler{lb: lb}

	// Middleware chain
	finalHandler := loggingMiddleware(
		rateLimitMiddleware(
			cacheMiddleware(gw.ServeHTTP), // perbaikan: langsung gunakan gw.ServeHTTP
		),
	)

	http.HandleFunc("/", finalHandler)
	log.Println("🚀 API Gateway dengan Multi-Backend & Load Balancing berjalan di :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
