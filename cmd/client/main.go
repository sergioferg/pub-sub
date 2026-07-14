package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	"github.com/joho/godotenv"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	godotenv.Load()
	port := os.Getenv("PORT")
	if port == "" {
		log.Fatal("error: PORT must be set")
	}

	connString := fmt.Sprintf("amqp://guest:guest@localhost:%s/", port)
	amqpConn, err := amqp.Dial(connString)
	if err != nil {
		log.Fatalf("error: couldn't make RabbitMQ connection; %v", err)
	}
	defer amqpConn.Close()

	fmt.Println("Connection successful!")

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("error: welcoming client failed; %v", err)
	}

	queueName := fmt.Sprintf("pause.%s", username)
	_, queue, err := pubsub.DeclareAndBind(
		amqpConn,
		routing.ExchangePerilDirect,
		queueName,
		routing.PauseKey,
		pubsub.Transient,
	)
	if err != nil {
		log.Fatalf("could not subscribe to pause: %v", err)
	}
	fmt.Printf("Queue %v declared and bound!\n", queue.Name)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	receivedSignal := <-sigChan

	fmt.Printf("\nReceived signal (%s). Program is shutting down...\n", receivedSignal)
}
