package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/munhwas1140/eventdrivenrabbit/internal"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	conn, err := internal.ConnectRabbitMQ("munhwas1140", "1234", "localhost:5671", "customers",
		"./tls-gen/basic/result/ca_certificate.pem",
		"./tls-gen/basic/result/client_nohyeongjun-ui-MacBookAir.local_certificate.pem",
		"./tls-gen/basic/result/client_nohyeongjun-ui-MacBookAir.local_key.pem",
	)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	// all consuming will be done on this Connection
	consumeConn, err := internal.ConnectRabbitMQ("munhwas1140", "1234", "localhost:5671", "customers",
		"./tls-gen/basic/result/ca_certificate.pem",
		"./tls-gen/basic/result/client_nohyeongjun-ui-MacBookAir.local_certificate.pem",
		"./tls-gen/basic/result/client_nohyeongjun-ui-MacBookAir.local_key.pem",
	)
	if err != nil {
		log.Fatal(err)
	}
	defer consumeConn.Close()

	client, err := internal.NewRabbitMQClient(conn)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	consumeClient, err := internal.NewRabbitMQClient(consumeConn)
	if err != nil {
		log.Fatal(err)
	}
	defer consumeClient.Close()

	queue, err := consumeClient.CreateQueue("", true, true)
	if err != nil {
		log.Fatal(err)
	}

	if err := consumeClient.CreateBinding(queue.Name, queue.Name, "customer_callbacks"); err != nil {
		log.Fatal(err)
	}

	messageBus, err := consumeClient.Consume(queue.Name, "customer-api", true, nil)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		for message := range messageBus {
			log.Printf("Message Callback %s\n", message.CorrelationId)
		}
	}()

	// if err := client.CreateQueue("customers_created", true, false); err != nil {
	// 	log.Fatal(err)
	// }
	// if err := client.CreateQueue("customers_test", false, true); err != nil {
	// 	log.Fatal(err)
	// }

	// if err := client.CreateBinding("customers_created", "customers.created.*", "customer_events"); err != nil {
	// 	log.Fatal(err)
	// }
	// if err := client.CreateBinding("customers_test", "customers.*", "customer_events"); err != nil {
	// 	log.Fatal(err)
	// }

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i := 0; i < 10; i++ {
		if err := client.Send(ctx, "customer_events", "customers.created.us", amqp.Publishing{
			ContentType:   "text/plain",
			DeliveryMode:  amqp.Persistent,
			ReplyTo:       queue.Name,
			CorrelationId: fmt.Sprintf("customer_created_%d", i),
			Body:          []byte(fmt.Sprintf("Message %d", i)),
		}); err != nil {
			log.Fatal(err)
		}

		// if err := client.Send(ctx, "customer_events", "customers.created.us", amqp.Publishing{
		// 	ContentType:  "text/plain",
		// 	DeliveryMode: amqp.Persistent,
		// 	Body:         []byte(fmt.Sprintf("Message num: %d", i)),
		// }); err != nil {
		// 	log.Fatal(err)
		// }

		// Sending a transient message
		// if err := client.Send(ctx, "customer_events", "customers.test", amqp.Publishing{
		// 	ContentType:  "text/plain",
		// 	DeliveryMode: amqp.Transient,
		// 	Body:         []byte(`An uncool undurable message`),
		// }); err != nil {
		// 	log.Fatal(err)
	}

	var blocking chan struct{}
	<-blocking

}
