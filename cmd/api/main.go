package main

import (
	"encoding/json"
	"net/http"

	"context"
	"fmt"
	"log"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/go-redis/redis/v8"
	"github.com/rabbitmq/amqp091-go"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Message struct {
	Sender   string `json:"sender"`
	Receiver string `json:"receiver"`
	Message  string `json:"message"`
}

func main() {

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	r := gin.Default()

	r.GET("/test", func(c *gin.Context) {
		c.JSON(200, "worked")
	})
	conn, err := amqp.Dial("amqp://user:password@localhost:7003/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	RBMQCH, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer RBMQCH.Close()

	r.POST("/messages", func(c *gin.Context) {
		SendMessages(RBMQCH, c)
	})

	r.GET("/messages", func(c *gin.Context) {
		ListMessages(rdb, c)
	})

	ProcessMessages(RBMQCH, rdb)

	r.Run(":8000")
}

// func newRBMQConnection() amqp.Channel {
// 	conn, err := amqp.Dial("amqp://user:password@localhost:7003/")
// 	failOnError(err, "Failed to connect to RabbitMQ")
// 	defer conn.Close()

// 	RBMQCH, err := conn.Channel()
// 	failOnError(err, "Failed to open a channel")
// 	defer RBMQCH.Close()

// 	return *RBMQCH
// }
func SendMessages(myRbmqh *amqp091.Channel, c *gin.Context) {
	var message Message
	if err := c.BindJSON(&message); err != nil {
		c.IndentedJSON(http.StatusBadRequest, gin.H{"message": err})
	}

	q, err := myRbmqh.QueueDeclare(
		"RabbitMQ", // name
		false,      // durable
		false,      // delete when unused
		false,      // exclusive
		false,      // no-wait
		nil,        // arguments
	)
	if err != nil {
		c.IndentedJSON(http.StatusBadRequest, gin.H{"message": err})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = myRbmqh.PublishWithContext(ctx,
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message.Message),
		})
	if err != nil {
		c.IndentedJSON(http.StatusBadRequest, gin.H{"message": err})
	}
	log.Printf(" [x] Sent %s\n", message.Message)

	c.IndentedJSON(http.StatusOK, message)
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func ProcessMessages(myRbmqh *amqp091.Channel, client *redis.Client) {
	q, err := myRbmqh.QueueDeclare(
		"RabbitMQ", // name
		false,      // durable
		false,      // delete when unused
		false,      // exclusive
		false,      // no-wait
		nil,        // arguments
	)
	if err != nil {
		failOnError(err, "Failed to process message")
	}
	msgs, err := myRbmqh.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		failOnError(err, "Failed to process message")
	}
	var forever chan struct{}

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
			// save in redis
			json, err := json.Marshal(Message{Message: string(d.Body)})
			if err != nil {
				fmt.Println(err)
			}
			m := &Message{
				Message: string(d.Body),
			}
			ctx := context.TODO()
			err = client.Set(ctx, json, 0).Err()
			if err != nil {
				fmt.Println(err)
			}

		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}

func ListMessages(client *redis.Client, c *gin.Context) {
	messages := make([]Message, 0)
}
