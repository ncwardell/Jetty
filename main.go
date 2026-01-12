package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/ncwardell/jetty/agent"
)

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
