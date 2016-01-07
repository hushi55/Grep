package main

import (
	"io"
	"os"
	"time"
	log "code.google.com/p/log4go"
)

type rdb_file_info struct {
	fullfilename string
	size		 int
}

var (
	rdbchan = make(chan rdb_file_info)
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
	
	t := time.Now()
	
	file_suffix := t.Format("2006-01-02T15:04")

	output := Conf.RedisRDBFilePath + "." + file_suffix
	
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
	
	rdbchan <- rdb_file_info{output, (replyLen + 2)}
	
	log.Info("redis master rdb size is %d", (replyLen + 2))
}
