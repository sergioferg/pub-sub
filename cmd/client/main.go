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

	publishCh, err := amqpConn.Channel()
	if err != nil {
		log.Fatal("error: failed to make publish channel;", err)
	}

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("error: welcoming client failed; %v", err)
	}
	gs := gamelogic.NewGameState(username)

	pauseQueueName := fmt.Sprintf("%s.%s", routing.PauseKey, gs.GetUsername())
	err = pubsub.SubscribeJSON(
		amqpConn,
		routing.ExchangePerilDirect,
		pauseQueueName,
		routing.PauseKey,
		pubsub.Transient,
		handlerPause(gs),
	)
	if err != nil {
		log.Fatalf("failed to subscribe to pause: %v", err)
	}

	err = pubsub.SubscribeJSON(
		amqpConn,
		routing.ExchangePerilTopic,
		routing.WarRecognitionsPrefix,
		routing.WarRecognitionsPrefix+".*",
		pubsub.Durable,
		handlerWar(gs),
	)
	if err != nil {
		log.Fatalf("could not subscribe to war declarations: %v", err)
	}

	moveQueueName := fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, gs.GetUsername())
	moveRoutingKey := fmt.Sprintf("%s.*", routing.ArmyMovesPrefix)
	err = pubsub.SubscribeJSON(
		amqpConn,
		routing.ExchangePerilTopic,
		moveQueueName,
		moveRoutingKey,
		pubsub.Transient,
		handlerMove(gs, publishCh),
	)
	if err != nil {
		log.Fatalf("failed to subscribe to move: %v", err)
	}

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}
		switch words[0] {
		case "spawn":
			err := gs.CommandSpawn(words)
			if err != nil {
				fmt.Println("wrong command usage;", err)
			} else {
				fmt.Println("successful spawn")
			}

		case "move":
			mv, err := gs.CommandMove(words)
			if err != nil {
				fmt.Println("wrong command usage;", err)
			}
			err = pubsub.PublishJSON(
				publishCh,
				routing.ExchangePerilTopic,
				moveRoutingKey,
				mv,
			)
			if err != nil {
				log.Fatal("error: couldn't publish json;", err)
			} else {
				fmt.Println("successful publish and move")
			}

		case "status":
			gs.CommandStatus()

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
