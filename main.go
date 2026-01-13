package main

import (
	"log"
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

// @schemes http https

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	a, err := agent.New()
	if err != nil {
		log.Fatalf("Failed to create agent: %v", err)
	}

	if err := a.Start(); err != nil {
		log.Fatalf("Failed to start agent: %v", err)
	}

	// Wait for shutdown signal
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Println("Shutting down...")
	a.Stop()
}
