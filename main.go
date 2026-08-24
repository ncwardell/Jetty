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

// Deliberately no @host: the spec is served from whichever node you reached,
// which may be a WARP IP, a LAN address, or a Cloudflare tunnel hostname.
// Pinning it to localhost:6880 made generated clients point at the wrong place
// everywhere except a local dev box. With host omitted, clients use the host
// they fetched the spec from.
// @BasePath /api

// @schemes https http

// @securityDefinitions.apikey ApiKeyAuth
// @in header
// @name X-API-Key
// @description API key for authentication (JETTY_SECRET)

// @Security ApiKeyAuth

// Keep docs/ in step with the @Router annotations. Version-pinned so the
// generator matches the swaggo/swag version in go.mod - a mismatch produces a
// spec the embedded UI cannot render. CI runs go generate and fails if
// anything under docs/ changes, so a new endpoint cannot land undocumented.
//
//go:generate go run github.com/swaggo/swag/cmd/swag@v1.16.6 init -g main.go -o docs --parseDependency --parseInternal

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
