package main

import (
	"bufio"
	log "code.google.com/p/log4go"
	"fmt"
	"net"
	"io"
	"strings"
	"time"
	"strconv"
//	"Grep/grep4r/redis"
)

type syncType int64

var (
	replAck = make(chan bool)
	networkRetryPsync = make(chan bool)
)

func sync(cmd Cmder, cn *conn) {

	log.Info("write cmd ......")
	cn.writeCmds(cmd)
	
	parseFullResync(cn)
	
	if cn.offset > 0 {
		writecp(&redisRepliInfo{cn.runid, cn.offset}, "sync init values")
	}
	
	log.Info("write cmd succuss")
	
	go func() {
		
		defer cn.Close()
		
		cptimer := newDeamonTimer(Conf.CheckPointTimeout)
		threshold := make(chan *redisRepliInfo)
		
		/**
		 * closure bind connection, connect error gorutine exit , connection close
		 */
		go func(){
			for {
				select {
				case <- replAck:
					redisReplicationACK(cn)
					
				case <- cptimer.timer.C :
					
					writecp(&redisRepliInfo{cn.runid, cn.offset + cn.GetReadCount()},
						 fmt.Sprintf("timeout %d millisecond", Conf.CheckPointTimeout/(1000*1000)))
					cptimer.reset()				
	
				case cp := <- threshold :
					writecp(cp, fmt.Sprintf("exceed threshold %d", Conf.CheckPointThreshold))
					
//				case <-time.After(time.Second * 1):
//					cn.writeCmds(ping)
				}
			}
		}()
		
		countReadByte := cn.GetReadCount()
		
		for count := uint64(0); ; count++ {

			log.Info("-------------------------------------- read from master: %d, read bytecount: %d", count, cn.GetReadCount())

			err := parseConnect(cn)
			
			// retry 
			if err == io.EOF {
				log.Error("remote %s redis master connect error, will retry", cn.RemoteAddr())
				networkRetryPsync <- true
				return 
			}
			
			if  uint64(cn.GetReadCount() - countReadByte) > Conf.CheckPointThreshold {
				
				countReadByte = cn.GetReadCount()
				threshold <- &redisRepliInfo{cn.runid, cn.offset + cn.GetReadCount()}
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

func parseFullResync(cn *conn) (bool, string, int64) {
	var (
		line []byte
	)

	line, _, err := cn.rd.ReadLine()
	
	if err != nil {
		log.Error("read message error: %s", err)
		return false, "?", -1
	}

	if len(line) == 0 || isNilReply(line) {
		log.Error("read nil reply message")
		return false, "?", -1
	}
	
	var(
		flag = string(line[0])
		parsemethod string
	)
	
	switch line[0] {
	case errorReply:
		err = parseErrorReply(cn, line)
		parsemethod = "parseErrorReply"
	case statusReply:
		val, err := parseStatusReply(cn, line)
		
		parsemethod = "parseStatusReply"
		
		if err == nil {
			if strings.HasPrefix(string(val), "FULLRESYNC") {
				items := strings.Split(string(val), " ")
				if len(items) == 3 {
					runid := items[1]
					offset, _ := strconv.ParseInt(items[2], 10, 64)
					writecp(&redisRepliInfo{runid, offset}, "FULLRESYNC init values")
					
					log.Info("read message flag: %s(%s), byte value is : %s", flag, parsemethod, val)
					
					cn.runid = runid
					cn.offset = offset
					
					return true, runid, offset
				}
			}
		}
	}
	
	log.Info("read message flag: %s(%s), byte value is : %s", flag, parsemethod, string(line))
	
	return false, "?", -1
}

// sync command
func fullsync() {

	log.Info("full sync cmd starting ...")
	cmd := NewStringCmd("SYNC")
//	cmd := NewStringCmd("PSYNC", "?", -1)

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
	
	conn.WriteTimeout = time.Minute * 30
	conn.ReadTimeout = time.Minute * 30

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
	
	// reset auth read bytes
	conn.ResetReadCount()
}

// psync command
func psync() {

	runid, offset := initRedisRepilcationInfo()
	
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

	// init runid offset
	conn := &conn{
		netcn: cn,
		buf:   make([]byte, 1024*1024*32),
		runid: runid,
		offset: offset,
	}
	conn.rd = bufio.NewReader(conn)
	
	conn.WriteTimeout = time.Minute * 30
	conn.ReadTimeout = time.Minute * 30
	
	redisAuth(conn)
	
	sync(cmd, conn)
	
}

func fullRDBFileParse() {

	for {
		select {
		case rdbinfo := <-rdbchan:
			go parserRDBFile(rdbinfo.fullfilename, rdbinfo.size)
		}
	}

}

func delta(val interface{}) {
	
	if val == nil {
		log.Warn("redis master delta command is nil")
		return ;
	}
	
	log.Info("redis master delta command is %s", val)
	
	switch vv := val.(type) {
	case []interface{}:
		
		if vstring, ok := vv[0].(string); ok {
			if strings.ToLower(vstring) == "ping" {
				replAck <- true
			}
		}
	}
}

func networkErrorRetryPsync() {
	for {
		select {
		case <- networkRetryPsync:
			psync()
		}
	}
}

func SyncDaemon() {
	
	/**
	 * try psync command
	 */
	psync()
	
	/**
	 * full data
	 */
	go fullRDBFileParse()
	
	/**
	 * network error retry
	 */
	go networkErrorRetryPsync()
}

func redisReplicationACK(cn *conn){
	log.Info("REPLCONF ACK %d, read count is %d", cn.offset, cn.GetReadCount())
	if (cn.offset > 0) {
		cn.writeCmds(NewStringCmd("REPLCONF", "ACK", cn.offset + cn.GetReadCount()))
	} else {
		cn.writeCmds(NewStringCmd("REPLCONF", "ACK", cn.GetReadCount()))
	}
}
