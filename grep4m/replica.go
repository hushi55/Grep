package grep4m

import (
	"fmt"
	"time"
	"strings"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	log "code.google.com/p/log4go"
)

//  we write to local.oplog.rs:
//     { ts : ..., h: ..., v: ..., op: ..., etc }
//   ts: an OpTime timestamp
//   h: hash
//   v: version
//   op:
//    "i" insert
//    "u" update
//    "d" delete
//    "c" db cmd
//    "db" declares presence of a database (ns is set to the db name + '.')
//    "n" no op
//   bb param:
//     if not null, specifies a boolean to pass along to the other side as b: param.
//     used for "justOne" or "upsert" flags on 'd', 'u'
type Oplog struct {
	H  uint64                 `json:"h"`
	Ns string                 `json:"ns"`
	O  map[string]interface{} `json:"o"`
	O2 map[string]interface{} `json:"o2"`
	Op string                 `json:"op"`
	Ts int64                  `json:"ts"`
	V  int                    `json:"v"`
}

func StartMongoReplica() {
	go OplogDeamon()
}

func OplogDeamon() {

	url := fmt.Sprintf("mongodb://%s:%s", Conf.MongoIP, Conf.MongoPort)
	session, err := mgo.Dial(url)
	if err != nil {
		panic(err)
	}
	defer session.Close()

	c := session.DB("local").C("oplog.rs")

	cp := initCheckpoint()

	log.Info("init check point is %d ", cp)

	iter := c.Find(bson.M{"ts": bson.M{"$gte": bson.MongoTimestamp(cp)}}).Tail(-1 * time.Second)
	defer iter.Close()

	count := uint64(0)
	for {

		log.Info("start oplog manarger !!! ")

		for result := new(Oplog); iter.Next(&result); result = new(Oplog) {

			count++
			checkpoint_threshold(count, result)

			if !checkTopicCollection(result.Ns) {
				log.Info("the namespace(%s) not in topics: %s", result.Ns, strings.Join(Conf.TopicCollections, "|"))
				continue
			}

			// new event
			newEvent(result)

		}

		if iter.Err() != nil {
			iter.Close()
		}
		if iter.Timeout() {
			continue
		}

		// reload checkp point from file
		cp = initCheckpoint()
		iter = c.Find(bson.M{"ts": bson.M{"$gte": bson.MongoTimestamp(cp)}}).Tail(-1 * time.Second)
	}

}

func checkpoint_threshold(count uint64, result *Oplog) {
	//	signal to checkpoint go
	if ((count & (Conf.CheckPointThreshold - 1)) == 0) && result != nil && result.Ts > 0 {
		checkpoint <- result.Ts
	}
}

func checkTopicCollection(ns string) bool {
	for _, item := range Conf.TopicCollections {
		if strings.Contains(strings.ToLower(ns), strings.ToLower(item)) {
			return true
		}
	}
	return false
}
