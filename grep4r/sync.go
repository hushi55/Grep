package main

import (
	log "code.google.com/p/log4go"
	"gopkg.in/redis.v3"
	"strconv"
	"strings"
	"time"
)

func queryRunid() (runid string, offset int64) {

	client := redis.NewClient(&redis.Options{
		Addr:     "192.168.22.111:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	cmd := redis.NewStringCmd("PSYNC", "?", -1)
	client.Process(cmd)
	v, err := cmd.Result()
	
	defer client.Close()

	runid = "?"
	offset = -1

	if err == nil {
		items := strings.Split(v, " ")
		if len(items) == 3 {
			runid = items[1]
			offset, _ = strconv.ParseInt(items[2], 10, 64)
		}
	} else {
		log.Info("psync error : %s", err)
	}

	log.Info("psync runid is %s, offset is %d", runid, offset)
	return
}


var (
	pongchan = make(chan bool)
)

func process(cmd Cmder, cn *conn) {

	cn.WriteTimeout = time.Minute * 30
	cn.ReadTimeout = time.Minute * 30

	log.Info("write cmd ......")
	
//	auth := newKeylessStatusCmd("AUTH", "xtkingdee")
//	cn.writeCmds(auth)
//	
	cn.writeCmds(cmd)
	log.Info("write cmd succuss")

	ping := NewStringCmd("PING")
	pong := "+PONG\r\n"
//	pong := "*1\r\n$4\r\nPONG\r\n"
	
//	okrepli := NewStringCmd("+OK")

	go func() {

		count := uint64(0)

		for {

			log.Info("read message from connection count is : %d", count)
			count++

			var (
				line []byte
				err  error
			)
			//			line = make([]byte, 4096)
			//			l, err := cn.Read(line)
			//			if l == 0 {
			//				continue
			//			}

			line, _, err = cn.rd.ReadLine()

			if len(line) == 0 || isNilReply(line) {
				log.Error("read nil reply message")
				continue
			}

			parseLine(line, cn)
			for cn.rd.Buffered() != 0 {
				log.Info("还存在数据")

				line, _, err = cn.rd.ReadLine()
				parseLine(line, cn)

				//				items, n, err := readScanStatusReply(cn)
				//				if err == nil {
				//					log.Info("read readScanReply imtes: %s, count is %d", items, n)
				//				} else {
				//					log.Error("readScanReply message error: %s", err)
				//				}

			}
			

			if err != nil {
				log.Error("read message error: %s", err)
			}
			
//			cn.writeCmds(okrepli)

			select {
			case <-pongchan:
				cn.Write([]byte(pong))
			case <-time.After(time.Second * 1):
				cn.writeCmds(ping)
			}

		}
	}()

}

func parseLine(line []byte, cn *conn) {
	
	if len(line) == 0 || isNilReply(line) {
		log.Warn("read nil reply message")
		return 
	}

	var (
		val interface{}
		err error
	)

	log.Error("read line[0]: %s", string(line[0]))
	switch line[0] {
	case errorReply:
		err = parseErrorReply(cn, line)
		log.Info("read parseErrorReply ")
	case statusReply:
		val, err = parseStatusReply(cn, line)
		log.Info("read parseStatusReply ")
	case intReply:
		val, err = parseIntReply(cn, line)
		log.Info("read parseIntReply ")
	case stringReply:
		val, err = parseBytesReply(cn, line)
		log.Info("read parseBytesReply ")
	case arrayReply:
		val, err = parseArrayReply(cn, sliceParser, line)
		log.Info("read parseArrayReply ")
		
		if err == nil {
			go func(interface{}){
				switch vv := val.(type) {
				case []interface{}:
					for i := 0; i < len(vv); i++ {
						log.Info("read message items : %s", vv[i])
						if vstring, ok := vv[i].(string); ok {
							if strings.ToLower(vstring) == "ping" {
								pongchan <- true
							}
						}
					}
				}
			}(val)
		}


	}
	if err != nil {
		log.Error("read message error: %s", err)
	}

	if v, ok := val.([]byte); ok {
		// Convert to string to preserve old behaviour.
		// TODO: remove in v4
		log.Info("read message value is : %s", v)
	} else {
		log.Info("read message other value is : %s", val)
	}
}