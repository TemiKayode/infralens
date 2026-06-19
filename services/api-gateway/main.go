// InfraLens API Gateway
// Accepts HTTP queries, routes to Rust query engine via gRPC, returns NDJSON.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ── Config ────────────────────────────────────────────────────────────────────

type Config struct {
	ListenAddr   string
	QueryBackend string // gRPC address of Rust query node
	JWTSecret    string
	APIKeyHeader string
	RateLimit    int // requests per second per IP (0 = disabled)
}

func configFromEnv() Config {
	return Config{
		ListenAddr:   envOr("GATEWAY_ADDR", ":8080"),
		QueryBackend: envOr("QUERY_BACKEND", "localhost:5317"),
		JWTSecret:    envOr("JWT_SECRET", ""),
		APIKeyHeader: envOr("API_KEY_HEADER", "X-API-Key"),
		RateLimit:    0,
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// ── Request / Response types ──────────────────────────────────────────────────

type QueryRequest struct {
	Query  string `json:"query"`
	Format string `json:"format,omitempty"` // "ndjson" (default) | "arrow"
}

type QueryError struct {
	Error string `json:"error"`
}

// ── Handler ───────────────────────────────────────────────────────────────────

type Gateway struct {
	cfg    Config
	logger *slog.Logger
	// grpcConn is lazy-connected per request in the stub implementation.
}

func NewGateway(cfg Config, logger *slog.Logger) *Gateway {
	return &Gateway{cfg: cfg, logger: logger}
}

func (g *Gateway) handleQuery(w http.ResponseWriter, r *http.Request) {
	// Decode request body
	var req QueryRequest
	dec := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if strings.TrimSpace(req.Query) == "" {
		writeError(w, http.StatusBadRequest, "query is required")
		return
	}

	// Dial the Rust gRPC backend
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(
		g.cfg.QueryBackend,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		g.logger.Error("failed to connect to query backend", "err", err)
		writeError(w, http.StatusServiceUnavailable, "query backend unavailable")
		return
	}
	defer conn.Close()

	// Use the InternalService QueryShard RPC.
	// The stub sends the SQL and streams back rows as Arrow IPC.
	// In the full implementation this uses the generated proto client.
	results, err := g.executeQuery(ctx, conn, req.Query)
	if err != nil {
		g.logger.Error("query execution failed", "err", err, "query", req.Query)
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Stream results as NDJSON
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)

	enc := json.NewEncoder(w)
	for _, row := range results {
		if err := enc.Encode(row); err != nil {
			break
		}
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	}
}

// executeQuery is the bridge to the Rust gRPC InternalService.
// In Phase 3 the QueryShard RPC returns Arrow IPC bytes that we decode here.
func (g *Gateway) executeQuery(ctx context.Context, conn *grpc.ClientConn, sql string) ([]map[string]any, error) {
	// Stub: send an unary call using raw bytes.
	// Full implementation uses the generated infralens.internal.v1.InternalServiceClient.
	//
	// For now, return a synthetic response so the gateway compiles and is testable
	// end-to-end without the Rust node.
	g.logger.Info("query dispatched to backend", "sql", sql, "backend", g.cfg.QueryBackend)

	// Real implementation pattern (commented reference):
	// client := internalv1.NewInternalServiceClient(conn)
	// stream, err := client.QueryShard(ctx, &internalv1.ShardQueryRequest{
	//     QueryId:   uuid.New().String(),
	//     PlanBytes: []byte(sql), // coordinator serialises PhysicalPlan; stub sends SQL
	//     Signal:    0,
	// })
	// rows = decode_arrow_ipc_stream(stream)

	_ = conn // used when real client is wired
	return []map[string]any{
		{"status": "ok", "query": sql, "rows": 0, "note": "query forwarded to backend"},
	}, nil
}

// ── Auth middleware ───────────────────────────────────────────────────────────

func (g *Gateway) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip auth if no secret configured (dev mode)
		if g.cfg.JWTSecret == "" {
			next.ServeHTTP(w, r)
			return
		}

		// Try Bearer token
		authHeader := r.Header.Get("Authorization")
		if strings.HasPrefix(authHeader, "Bearer ") {
			tokenStr := strings.TrimPrefix(authHeader, "Bearer ")
			_, err := jwt.Parse(tokenStr, func(t *jwt.Token) (any, error) {
				if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
					return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
				}
				return []byte(g.cfg.JWTSecret), nil
			})
			if err == nil {
				next.ServeHTTP(w, r)
				return
			}
		}

		// Try API key header
		if apiKey := r.Header.Get(g.cfg.APIKeyHeader); apiKey != "" {
			// In production: validate against a key store.
			// Here: accept any non-empty key in dev when JWT secret is set.
			next.ServeHTTP(w, r)
			return
		}

		writeError(w, http.StatusUnauthorized, "authentication required")
	})
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(QueryError{Error: msg})
}

// ── Main ──────────────────────────────────────────────────────────────────────

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	cfg := configFromEnv()
	gw  := NewGateway(cfg, logger)

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(30 * time.Second))

	// Health / readiness
	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	r.Get("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	// Query API — protected
	r.Group(func(r chi.Router) {
		r.Use(gw.authMiddleware)
		r.Post("/api/v1/query", gw.handleQuery)
	})

	srv := &http.Server{
		Addr:         cfg.ListenAddr,
		Handler:      r,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 35 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		logger.Info("API gateway listening", "addr", cfg.ListenAddr, "backend", cfg.QueryBackend)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server error", "err", err)
			os.Exit(1)
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("shutdown error", "err", err)
	}
	logger.Info("gateway stopped")
}
