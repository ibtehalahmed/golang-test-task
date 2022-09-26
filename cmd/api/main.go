package main

import (
	"net/http"

	"context"
	"log"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/rabbitmq/amqp091-go"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Message struct {
	Sender   string `json:"sender"`
	Receiver string `json:"receiver"`
	Message  string `json:"message"`
}

func main() {

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
