package main

import (
	"bufio"
	log "code.google.com/p/log4go"
	"net"
	"time"
)

/*

xuntong.cache.redis.host=192.168.22.111
xuntong.cache.redis.port=6379
xuntong.cache.redis.password=xtkingdee

*/

func main() {

//	runid, offset := queryRunid()

	log.Info("psync cmd starting ...")
//	cmd := NewStringCmd("PSYNC", runid, offset)
	cmd := NewStringCmd("SYNC")

	cn, err := net.DialTimeout("tcp", "192.168.22.111:6379", time.Minute*30)
	
	if err != nil {
		log.Error("connect master error : %s", err)
	}
	
	

	time.Sleep(time.Second * 5)

	conn := &conn{
		netcn: cn,
		buf:   make([]byte, 1024*1024*32),
	}
	conn.rd = bufio.NewReader(conn)
	
	process(cmd, conn)

	InitSignal()
}

