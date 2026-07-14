package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	"github.com/joho/godotenv"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")
	godotenv.Load()
	port := os.Getenv("PORT")
	if port == "" {
		log.Fatal("error: PORT must be set")
	}

	conString := fmt.Sprintf("amqp://guest:guest@localhost:%s/", port)
	fmt.Println(conString)
	amqpConn, err := amqp.Dial(conString)
	if err != nil {
		log.Fatal("error: couldn't make amqp connection;", err)
	}
	defer amqpConn.Close()

	amqpChan, err := amqpConn.Channel()
	if err != nil {
		log.Fatal("error: couldn't make amqp channel;", err)
	}

	fmt.Println("Connection successful!")

	err = pubsub.PublishJSON(amqpChan, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
		IsPaused: true,
	})
	if err != nil {
		log.Fatal("error: couldn't publish json;", err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	receivedSignal := <-sigChan

	fmt.Printf("\nReceived signal (%s). Program is shutting down...\n", receivedSignal)
}
