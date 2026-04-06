package main

import (
	"btrl/internal/routers"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func initLog() *zap.Logger {
	cfg := zap.NewDevelopmentConfig()
	cfg.Encoding = "console"
	cfg.EncoderConfig.TimeKey = ""
	cfg.DisableStacktrace = true
	cfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	cfg.EncoderConfig.ConsoleSeparator = " | "

	log, err := cfg.Build()
	if err != nil {
		panic(err)
	}

	return log
}

func main() {
	l := initLog()

	r := routers.NewServer(l, nil, "")
	if r == nil {
		l.Fatal("Failed to create server")
	}

	if err := r.ListenAndServe(); err != nil {
		l.Fatal("Failed to start server", zap.Error(err))
	}
}
