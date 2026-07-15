package pubsub

import (
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	Durable SimpleQueueType = iota
	Transient
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	connChannel, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("failed to open channel: %w", err)
	}

	connQueue, err := connChannel.QueueDeclare(
		queueName,            // name
		queueType == Durable, // durable
		queueType != Durable, // delete when unused
		queueType != Durable, // exclusive
		false,                // no-wait
		nil,                  // args
	)
	if err != nil {
		connChannel.Close() // Prevent channel leak on error
		return nil, amqp.Queue{}, fmt.Errorf("queue declare failed: %w", err)
	}

	err = connChannel.QueueBind(
		connQueue.Name, // queue name
		key,            // routing key
		exchange,       // exchange
		false,          // no-wait
		nil,            // args
	)
	if err != nil {
		connChannel.Close()
		return nil, amqp.Queue{}, fmt.Errorf("queue bind failed: %w", err)
	}

	return connChannel, connQueue, nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T),
) error {
	ch, queue, err := DeclareAndBind(
		conn,
		exchange,
		queueName,
		key,
		queueType,
	)
	if err != nil {
		return fmt.Errorf("failed to declare and bind queue: %v", err)
	}

	fmt.Printf("Queue %v declared and bound!\n", queue.Name)

	msgs, err := ch.Consume(
		queue.Name, // queue
		"",         // consumer
		false,      // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	if err != nil {
		return fmt.Errorf("failed to register a consumer: %v", err)
	}

	unmarshaller := func(data []byte) (T, error) {
		var target T
		err := json.Unmarshal(data, &target)
		return target, err
	}

	go func() {
		defer ch.Close()
		for msg := range msgs {
			target, err := unmarshaller(msg.Body)
			if err != nil {
				fmt.Printf("could not unmarshal message: %v\n", err)
				continue
			}
			handler(target)
			msg.Ack(false)
		}
	}()

	return nil
}
