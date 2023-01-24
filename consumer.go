package main

import (
	"context"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go" // Делаем удобное имя для импорта в нашем коде
	"log"
	"time"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/") // Создаем подключение к RabbitMQ
	failOnError(err, "Failed to connect to RabbitMQ")

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")

	q, err := ch.QueueDeclare(
		"hello", // name
		false,   // durable
		true,    // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)

	failOnError(err, "Failed to declare a queue")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	for i := 0; i <= 20; i++ {
		body := fmt.Sprintf("Worker %d", i)
		err = ch.PublishWithContext(ctx,
			"",     // exchange
			q.Name, // routing key
			false,  // mandatory
			false,  // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(body),
			})
		failOnError(err, "Failed to publish a message")

		log.Printf(" [x] Sent %s\n", body)
		time.Sleep(2 * time.Second)
	}

	defer func() {
		cancel()
		_ = conn.Close() // Закрываем подключение в случае удачной попытки
		_ = ch.Close()   // Закрываем канал в случае удачной попытки открытия
	}()
}
