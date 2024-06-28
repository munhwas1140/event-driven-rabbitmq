package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/munhwas1140/eventdrivenrabbit/internal"
	amqp "github.com/rabbitmq/amqp091-go"
	"golang.org/x/sync/errgroup"
)

var (
	consumer = flag.String("consumer", "consumer", "consumer")
	priority = flag.Int("priority", 1, "priority")
)

func init() {
	flag.Parse()
}

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

	publishConn, err := internal.ConnectRabbitMQ("munhwas1140", "1234", "localhost:5671", "customers",
		"./tls-gen/basic/result/ca_certificate.pem",
		"./tls-gen/basic/result/client_nohyeongjun-ui-MacBookAir.local_certificate.pem",
		"./tls-gen/basic/result/client_nohyeongjun-ui-MacBookAir.local_key.pem",
	)
	if err != nil {
		log.Fatal(err)
	}
	defer publishConn.Close()

	client, err := internal.NewRabbitMQClient(conn)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	publishClient, err := internal.NewRabbitMQClient(publishConn)
	if err != nil {
		log.Fatal(err)
	}
	defer publishClient.Close()

	var blocking chan struct{}

	q, err := client.CreateQueue("", true, true)
	if err != nil {
		log.Fatal(err)
	}

	if err := client.CreateBinding(q.Name, "", "customer_events"); err != nil {
		log.Fatal(err)
	}
	messageBus, err := client.Consume(q.Name, "consumer", false, nil)
	if err != nil {
		log.Fatal(err)
	}

	// apply hard limit on the server
	if err := client.ApplyQos(10, 0, true); err != nil {
		log.Fatal(err)
	}

	g, ctx := errgroup.WithContext(context.Background())
	g.SetLimit(10)
	go func() {
		for message := range messageBus {
			// Spawn a worker
			msg := message
			g.Go(func() error {
				log.Printf("New message: %s, consumer: %s", string(msg.Body), *consumer)
				time.Sleep(10 * time.Second)
				if err := msg.Ack(false); err != nil {
					log.Println("Ack message failed")
					return err
				}
				if err := publishClient.Send(ctx, "customer_callbacks", msg.ReplyTo, amqp.Publishing{
					ContentType:   "text/plain",
					DeliveryMode:  amqp.Persistent,
					Body:          []byte("RPC COMPLETE"),
					CorrelationId: msg.CorrelationId,
				}); err != nil {
					log.Fatal(err)
				}
				log.Printf("Acknowledged message %s\n", message.MessageId)
				return nil
			})
		}
	}()

	// defaultConsuming(client, "customers_created", "consumer1", false, amqp.Table{
	// 	"x-priority": *priority,
	// })
	// defaultConsuming(client, "customers_created", "consumer2", false, amqp.Table{
	// 	"x-priority": *priority,
	// })
	log.Println("Consuming, use CTRL+C to exit")
	<-blocking
}

func defaultConsuming(client internal.RabbitClient, queue, consumer string, autoAck bool, args amqp.Table) error {
	// messageBus, err := client.Consume(queue, consumer, autoAck, args)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	q, err := client.CreateQueue("", true, true)
	if err != nil {
		log.Fatal(err)
	}

	if err := client.CreateBinding(q.Name, "", "customer_events"); err != nil {
		log.Fatal(err)
	}
	messageBus, err := client.Consume(q.Name, consumer, autoAck, args)
	if err != nil {
		log.Fatal(err)
	}

	g, _ := errgroup.WithContext(context.Background())
	// errgroup allow us concurrent task
	g.SetLimit(10)
	go func() {
		for message := range messageBus {
			// Spawn a worker
			msg := message
			g.Go(func() error {
				log.Printf("New message: %s, consumer: %s", string(msg.Body), consumer)
				time.Sleep(10 * time.Second)
				if err := msg.Ack(false); err != nil {
					log.Println("Ack message failed")
					return err
				}
				log.Printf("Acknowledged message %s\n", message.MessageId)
				return nil
			})
		}
	}()

	return nil
}
