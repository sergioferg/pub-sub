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
	fmt.Println("Starting Peril server...")
	godotenv.Load()
	port := os.Getenv("PORT")
	if port == "" {
		log.Fatal("error: PORT must be set")
	}

	connString := fmt.Sprintf("amqp://guest:guest@localhost:%s/", port)
	fmt.Println(connString)
	amqpConn, err := amqp.Dial(connString)
	if err != nil {
		log.Fatal("error: couldn't make amqp connection;", err)
	}
	defer amqpConn.Close()

	publishCh, err := amqpConn.Channel()
	if err != nil {
		log.Fatal("error: couldn't make amqp channel;", err)
	}

	fmt.Println("Connection successful!")

	routingKey := fmt.Sprintf("%s.*", routing.GameLogSlug)
	err = pubsub.SubscribeGob(
		amqpConn,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		routingKey,
		pubsub.Durable,
		handlerLog(),
	)
	if err != nil {
		log.Fatalf("could not subscribe to logs: %v", err)
	}

	gamelogic.PrintServerHelp()

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}
		switch words[0] {
		case "pause":
			fmt.Println("Pausing the game...")
			err = pubsub.PublishJSON(
				publishCh,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: true,
				},
			)
			if err != nil {
				log.Fatal("error: couldn't publish json;", err)
			}
		case "resume":
			fmt.Println("Resuming the game...")
			err = pubsub.PublishJSON(
				publishCh,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: false,
				},
			)
			if err != nil {
				log.Fatal("error: couldn't publish json;", err)
			}
		case "quit":
			fmt.Printf("Program is shutting down...\n")
			return
		default:
			fmt.Printf("unknown command\n")
		}
	}
}
