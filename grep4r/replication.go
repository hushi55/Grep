package main

import (
	"io"
	"os"
	log "code.google.com/p/log4go"
)

var (
	rdbchan = make(chan int)
)

func openReadFile(name string) (*os.File, int64) {
	f, err := os.Open(name)
	if err != nil {
		log.Error("cannot open file-writer '%s', err : ", name, err)
	}
	s, err := f.Stat()
	if err != nil {
		log.Error("cannot open file-writer '%s', err : ", name, err)
	}
	return f, s.Size()
}

func openWriteFile(name string) *os.File {
	f, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		log.Error("cannot open file-writer '%s', err : ", name, err)
	}
	return f
}

func writeDumpRDBFile(replyLen int, cn *conn) {

	buffsize := 4 * 1024 * 1024

	output := "./repli.dmp"
	var dumpto io.WriteCloser
	if output != "/dev/stdout" {
		dumpto = openWriteFile(output)
		defer dumpto.Close()
	} else {
		dumpto = os.Stdout
	}

	for i, count := 0, 0; ; i++ {
		length := replyLen + 2 - count
		if length <= 0 {
			break
		}

		if length > buffsize {
			length = buffsize
		}

		b, err := readN(cn, length)
		count += len(b)
		if err != nil {
			log.Error("write dump rdb file err: %s", err)
			return
		}
		dumpto.Write(b)
	}
	
	rdbchan <- (replyLen + 2)
	
	log.Info("redis master rdb size is %d", (replyLen + 2))
}
