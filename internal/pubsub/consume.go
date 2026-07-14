package pubsub

import (
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
