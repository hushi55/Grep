package main

import (
	log "code.google.com/p/log4go"
	"encoding/json"
	"fmt"
	"github.com/streadway/amqp"
	"math/rand"
)

const (
	EXCHANGE_KEY  string = "search_redis_event_topic_exchange"
	EXCHANGE_TYPE string = "topic"
)

var (
	PRODUCTER_CONN    *amqp.Connection
	PRODUCTER_CHANNEL *amqp.Channel
	channel_pool      [16]*amqp.Channel
)

type event struct {
	channel *amqp.Channel
	val     *interface{}
}

func InitEvent() {
	url := fmt.Sprintf("amqp://%s:%s@%s:%s/", Conf.AmqpUser, Conf.AmqpPasswd, Conf.AmqpIp, Conf.AmqpPort)
	PRODUCTER_CONN, err := amqp.Dial(url)
	failOnError(err, "Failed to connect to RabbitMQ")

	PRODUCTER_CHANNEL, err := PRODUCTER_CONN.Channel()
	failOnError(err, "Failed to open a channel")

	i := 0
	for i < len(channel_pool) {

		log.Info("make a new channel index is : %d", i)

		c, err := PRODUCTER_CONN.Channel()
		if err == nil {

			c.ExchangeDeclare(
				EXCHANGE_KEY,  // name
				EXCHANGE_TYPE, // type
				true,          // durable
				false,         // auto-deleted
				false,         // internal
				false,         // no-wait
				nil,           // arguments
			)

			channel_pool[i] = c
			i++
		} else {
			failOnError(err, "Failed to open a channel")
		}
	}

	err = PRODUCTER_CHANNEL.ExchangeDeclare(
		EXCHANGE_KEY,  // name
		EXCHANGE_TYPE, // type
		true,          // durable
		false,         // auto-deleted
		false,         // internal
		false,         // no-wait
		nil,           // arguments
	)
	failOnError(err, "Failed to declare an exchange")
}

func ShutdownEvent() {

	PRODUCTER_CHANNEL.Close()
	for _, c := range channel_pool {
		if c != nil {
			c.Close()
		}
	}
	PRODUCTER_CONN.Close()

}

func (this *event) run() {
	go produce(PRODUCTER_CONN, this.channel, this.val)
}

func produce(conn *amqp.Connection, channel *amqp.Channel, val *interface{}) {

	if val == nil {
		log.Warn("the redis json is nil")
		return
	}

	body, err := json.Marshal(val)

	if err != nil || body == nil {
		log.Error("redis event to json error: %s , oplog is : %s ", err, string(body))
	} else {

		routingKey := "redis.event"
		log.Info("routing key is : %s ", routingKey)

		err = channel.Publish(
			EXCHANGE_KEY, // exchange
			routingKey,   // routing key
			false,        // mandatory
			false,        // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        body,
			})
		if err != nil {
			log.Error("publish message err : %s ", err)
		}

		//TODO recreate channel ?
	}
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Error("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func newEvent(val interface{}) {

	if val == nil {
		log.Warn("event is nil")
		return
	}

	rand.Seed(1198)
	// rand channel
	e := &event{val: &val, channel: channel_pool[rand.Intn(len(channel_pool))]}
	e.run()

}
