package main

import (
	"fmt"
	"log"
	"os"

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

	gameState := gamelogic.NewGameState(username)

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}
		switch words[0] {
		case "spawn":
			err := gameState.CommandSpawn(words)
			if err != nil {
				fmt.Println("wrong command usage;", err)
			} else {
				fmt.Println("successful spawn")
			}

		case "move":
			_, err := gameState.CommandMove(words)
			if err != nil {
				fmt.Println("wrong command usage;", err)
			} else {
				fmt.Println("successful move")
			}

		case "status":
			gameState.CommandStatus()

		case "help":
			gamelogic.PrintClientHelp()

		case "spam":
			fmt.Println("Spamming not allowed yet!")

		case "quit":
			gamelogic.PrintQuit()
			return

		default:
			fmt.Println("unknown command")
		}
	}
}
