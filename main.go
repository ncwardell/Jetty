package main

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/ncwardell/jetty/agent"

	_ "github.com/ncwardell/jetty/docs" // swagger docs
)

// @title Jetty API
// @version 2.0
// @description P2P Docker Compose orchestration with Cloudflare WARP mesh networking
// @termsOfService http://swagger.io/terms/

// @contact.name Jetty Support
// @contact.url https://github.com/ncwardell/jetty

// @license.name MIT
// @license.url https://opensource.org/licenses/MIT

// @host localhost:6880
// @BasePath /api

// @schemes https http

// @securityDefinitions.apikey ApiKeyAuth
// @in header
// @name X-API-Key
// @description API key for authentication (JETTY_SECRET)

// @Security ApiKeyAuth

func main() {
	// First thing, so nothing logs before the level and format are settled.
	agent.InitLogging()

	a, err := agent.New()
	if err != nil {
		slog.Error("failed to create agent", "err", err)
		os.Exit(1)
	}

	if err := a.Start(); err != nil {
		slog.Error("failed to start agent", "err", err)
		os.Exit(1)
	}

	// Wait for shutdown signal
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	slog.Info("shutting down")
	a.Stop()
}
