package main

import (
	"context"
	"log"
	"time"

	"github.com/munhwas1140/eventdrivenrabbit/internal"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	conn, err := internal.ConnectRabbitMQ("munhwas1140", "1234", "localhost:5672", "customers")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	client, err := internal.NewRabbitMQClient(conn)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	if err := client.CreateQueue("customers_created", true, false); err != nil {
		log.Fatal(err)
	}
	if err := client.CreateQueue("customers_test", false, true); err != nil {
		log.Fatal(err)
	}

	if err := client.CreateBinding("customers_created", "customers.created.*", "customer_events"); err != nil {
		log.Fatal(err)
	}
	if err := client.CreateBinding("customers_test", "customers.*", "customer_events"); err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Send(ctx, "customer_events", "customers.created.us", amqp.Publishing{
		ContentType:  "text/plain",
		DeliveryMode: amqp.Persistent,
		Body:         []byte(`An cool message between services`),
	}); err != nil {
		log.Fatal(err)
	}

	// Sending a transient message
	if err := client.Send(ctx, "customer_events", "customers.test", amqp.Publishing{
		ContentType:  "text/plain",
		DeliveryMode: amqp.Transient,
		Body:         []byte(`An uncool undurable message`),
	}); err != nil {
		log.Fatal(err)
	}

	time.Sleep(10 * time.Second)

	log.Println(client)

}
