package main

import (

	"os"
	"os/signal"


	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)


func main () {
	logger := logrus.New()
	logger.SetOutput(os.Stdout)
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
		DisableColors: false,
	})
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5673/")
	if err != nil {
		logger.WithError(err).Fatal("can not connect to rabbitmq")
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		logger.WithError(err).Fatal("can not create channel in rabbitmq")
	}
	q, err := ch.QueueDeclare(
		"hello", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	if err != nil {
		logger.WithError(err).Fatal("can not queue declare")
	}
	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)

	go func() {
		for d := range msgs {
			logger.WithFields(logrus.Fields{
				"body": string(d.Body),
				"id": d.MessageId,
			}).Info("new message came")
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit
	logger.Info("try to shutdown the app")
	err = ch.Close()
	if err != nil {
		logger.WithError(err).Warn("can not shutdown router")
	}
	logger.Info("shutdown process was successful")
}