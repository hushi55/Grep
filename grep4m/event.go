package main

import (
	log "code.google.com/p/log4go"
	"encoding/json"
	"fmt"
	"github.com/streadway/amqp"
	"gopkg.in/mgo.v2/bson"
	"math/rand"
)

const (
	EXCHANGE_KEY  string = "search_event_topic_exchange"
	EXCHANGE_TYPE string = "topic"
)

var (
	PRODUCTER_CONN    *amqp.Connection
	PRODUCTER_CHANNEL *amqp.Channel
	channel_pool      [16]*amqp.Channel
)

type event struct {
	channel *amqp.Channel
	oplog   *Oplog
}

//   op:
//    "i" insert
//    "u" update
//    "d" delete
//    "c" db cmd
//    "db" declares presence of a database (ns is set to the db name + '.')
//    "n" no op
// monogdb event
type mevent struct {
	Ns string `json:"ns"`
	Op string `json:"op"`
	Id string `json:"id"`
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
	go produce(PRODUCTER_CONN, this.channel, this.oplog)
}

func produce(conn *amqp.Connection, channel *amqp.Channel, oplog *Oplog) {

	if oplog == nil {
		log.Warn("the oplog is nil")
		return
	}

	me := convert(oplog)

	body, err := json.Marshal(*me)

	if err != nil {
		log.Error("format oplog to json error: %s , oplog is : %s ", err, string(body))
	} else {

		op, _ := json.Marshal(*oplog)
		log.Info("oplog is : %s ", string(op))
		log.Info("mongodb evnet is : %s ", string(body))
		
		routingKey := "mongodb." + oplog.Ns
		log.Info("routing key is : %s ", routingKey)

		err = channel.Publish(
			EXCHANGE_KEY, 			// exchange
			routingKey,           // routing key
			false,        // mandatory
			false,        // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(body),
			})
		if err != nil {
			log.Error("publish message err : %s ", err)
		}

		//TODO recreate channel ?
	}
}

func convert(oplog *Oplog) *mevent {

	if oplog == nil {
		return nil
	}

	me := new(mevent)
	me.Ns = oplog.Ns
	me.Op = oplog.Op

	if oplog.Op == "u" && oplog.O2 != nil { //
		me.Id = readId(oplog.O2)
	} else if oplog.O != nil {
		me.Id = readId(oplog.O)
	}

	return me
}

func readId(m map[string]interface{}) string {
	if v, ok := m["_id"]; ok {
		if id, ok2 := v.(string); ok2 {
			return id
		} else if id, ok2 := v.(bson.ObjectId); ok2 {
			return fmt.Sprintf("%x", string(id))
		} else {
			log.Info("can not _id field to string")
		}
	}
	return ""
}

func checkTopicUpdateField(oplog *Oplog) bool{
//	if oplog.Op == "u" {
//		
//	}
	return true
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Error("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func newEvent(context *Oplog) {
	rand.Seed(1198)
	// rand channel
	e := &event{oplog: context, channel: channel_pool[rand.Intn(len(channel_pool))]}
	e.run()
}
