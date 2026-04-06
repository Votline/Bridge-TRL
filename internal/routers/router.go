// Package routers creates a Server struct with registered workers.
package routers

import (
	"net/http"

	wrks "btrl/internal/workers"

	"go.uber.org/zap"
)

// Server is a http server with zap ogger
type Server struct {
	log *zap.Logger
	Srv *http.Server
}

// NewServer creates a new server with registered workers on port
func NewServer(log *zap.Logger, workers []wrks.Worker, port string) *Server {
	m := http.NewServeMux()

	if workers == nil {
		workers = make([]wrks.Worker, 0, 1)
	}
	if port == "" {
		port = ":8080"
	}

	workers = append(workers, wrks.NewTranslate(log))

	for i, wrk := range workers {
		wrk.Register(m)
		log.Info("Registered worker",
			zap.Int("index", i),
			zap.String("name", wrk.GetName()))
	}
	return &Server{
		log: log,
		Srv: &http.Server{
			Addr:    port,
			Handler: m,
		},
	}
}

func (s *Server) ListenAndServe() error {
	s.log.Info("Starting server",
		zap.String("port", s.Srv.Addr))
	return s.Srv.ListenAndServe()
}
