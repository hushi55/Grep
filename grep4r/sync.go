package main

import (
	"bufio"
	log "code.google.com/p/log4go"
	"fmt"
	"gopkg.in/redis.v3"
	"net"
	"io"
	"strconv"
	"strings"
	"time"
)

func queryRunid() (runid string, offset int64) {

	addr := fmt.Sprintf("%s:%s", Conf.RedisMasterIP, Conf.RedisMasterPort)
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: Conf.RedisMasterPasswd, // no password set
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

func sync(cmd Cmder, cn *conn) {

	cn.WriteTimeout = time.Minute * 30
	cn.ReadTimeout = time.Minute * 30

	log.Info("write cmd ......")
	cn.writeCmds(cmd)
	log.Info("write cmd succuss")

	ping := NewStringCmd("PING")
	pong := "+PONG\r\n"

	/**
	 * full data
	 */
	go full()

	go func() {
		
		defer cn.Close()
		
		for count := uint64(0); ; count++ {

			log.Info("-------------------------------------- read from master: %d", count)

			err := parseConnect(cn)
			
			// retry 
			if err == io.EOF {
				RetryPsync <- true
				return 
			}
			

			select {
			case <-pongchan:
				cn.Write([]byte(pong))
			case <-time.After(time.Second * 1):
				cn.writeCmds(ping)
			}

		}
	}()
	
	

}

func parseConnect(cn *conn) (err error) {

	var (
		line []byte
	)

	line, _, err = cn.rd.ReadLine()
	
	if err != nil {
		log.Error("read message error: %s", err)
		return 
	}

	if len(line) == 0 || isNilReply(line) {
		log.Error("read nil reply message")
		return
	}

	parseLine(line, cn)
	for cn.rd.Buffered() != 0 {

		log.Info("connect buffer's exist data for read")

		line, _, err = cn.rd.ReadLine()
		parseLine(line, cn)

	}
	
	return 
}

func parseLine(line []byte, cn *conn) {

	if len(line) == 0 || isNilReply(line) {
		log.Warn("read nil reply message")
		return
	}

	var (
		val         interface{}
		err         error
		parsemethod string
		flag        string
	)

	flag = string(line[0])
	switch line[0] {
	case errorReply:
		err = parseErrorReply(cn, line)
		parsemethod = "parseErrorReply"
	case statusReply:
		val, err = parseStatusReply(cn, line)
		parsemethod = "parseStatusReply"
	case intReply:
		val, err = parseIntReply(cn, line)
		parsemethod = "parseIntReply"
	case stringReply:
		val, err = parseBytesReply(cn, line)
		parsemethod = "parseBytesReply"
	case arrayReply:
		val, err = parseArrayReply(cn, sliceParser, line)
		parsemethod = "parseArrayReply"

		if err == nil {
			go delta(val)
		}

	}

	if err != nil {
		log.Error("read message flag: %s(%s), error: %s", flag, parsemethod, err)
	} else {
		if v, ok := val.([]byte); ok {
			// Convert to string to preserve old behaviour.
			// TODO: remove in v4
			log.Info("read message flag: %s(%s), byte value is : %s", flag, parsemethod, v)
		} else {
			log.Info("read message flag: %s(%s), other value is : %s", flag, parsemethod, val)
		}
	}

}

// sync command
func fullsync() {

	log.Info("full sync cmd starting ...")
	cmd := NewStringCmd("SYNC")

	addr := fmt.Sprintf("%s:%s", Conf.RedisMasterIP, Conf.RedisMasterPort)
	cn, err := net.DialTimeout("tcp", addr, time.Minute*30)

	if err != nil {
		log.Error("connect master error : %s", err)
	} else {
		log.Info("connect redis master : %s", addr)
	}

	time.Sleep(time.Second * 5)

	conn := &conn{
		netcn: cn,
		buf:   make([]byte, 1024*1024*32),
	}
	conn.rd = bufio.NewReader(conn)

	redisAuth(conn)

	sync(cmd, conn)
}

func redisAuth(conn *conn) {
	// redis auth
	if Conf.RedisMasterPasswd != "" {
		auth := newKeylessStatusCmd("AUTH", Conf.RedisMasterPasswd)
		conn.writeCmds(auth)

		time.Sleep(time.Second * 2)

		parseConnect(conn)
	}
}

// psync command
func psync() {

	runid, offset := queryRunid()

	log.Info("psync cmd starting ...")
	cmd := NewStringCmd("PSYNC", runid, offset)

	addr := fmt.Sprintf("%s:%s", Conf.RedisMasterIP, Conf.RedisMasterPort)
	cn, err := net.DialTimeout("tcp", addr, time.Minute*30)

	if err != nil {
		log.Error("connect master error : %s", err)
	} else {
		log.Info("connect redis master : %s", addr)
	}

	time.Sleep(time.Second * 5)

	conn := &conn{
		netcn: cn,
		buf:   make([]byte, 1024*1024*32),
	}
	conn.rd = bufio.NewReader(conn)
	
	redisAuth(conn)

	sync(cmd, conn)
}

func full() {

	for {
		select {
		case rdbinfo := <-rdbchan:
			go parserRDBFile(rdbinfo.fullfilename, rdbinfo.size)
		}
	}

}

func delta(val interface{}) {
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
}
