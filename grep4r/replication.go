package main

import (
	"io"
	"os"
	"time"
	"strings"
	log "code.google.com/p/log4go"
)

type rdb_file_info struct {
	fullfilename string
	size		 uint64
}

var (
	rdbchan = make(chan *rdb_file_info)
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
	
	file_suffix := t.Format("2006-01-02T15-04")

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
	
	rdbFileWriteSuccess(output, uint64(replyLen+2), cn)
}


/* 
 * There are two possible forms for the bulk payload. One is the
 * usual $<count> bulk format. The other is used for diskless transfers
 * when the master does not know beforehand the size of the file to
 * transfer. In the latter case, the following format is used:
 *
 * $EOF:<40 bytes delimiter>
 *
 * At the end of the file the announced delimiter is transmitted. The
 * delimiter is long and random enough that the probability of a
 * collision with the actual file content can be ignored. 
 */
func writeDumpRDBFileDiskless(eof string, cn *conn) {

	buffsize := 4 * 1024 * 1024
	
	t := time.Now()
	
	file_suffix := t.Format("2006-01-02T15-04")

	output := Conf.RedisRDBFilePath + "." + file_suffix
	
	var dumpto io.WriteCloser
	if output != "/dev/stdout" {
		dumpto = openWriteFile(output)
		defer dumpto.Close()
	} else {
		dumpto = os.Stdout
	}

	replyLen := uint64(0)
	
	eofFlag 	:= eof
	eofFlagLen 	:= len(eofFlag)
	logmask		:= power2(1000)-1
	
	log.Info("full eof length is %d, eof is %s", eofFlagLen, eof)
	
	for i := uint64(0); ;i++ {
		b, writeLen, err := readAtMost(cn, buffsize)
		
		if err != nil {
			log.Error("write dump rdb file err: %s", err)
			return
		}
		
		if cn.rd.Buffered() == 0 && strings.HasSuffix(bytesToString(b[:writeLen]), eof) {
			if writeLen > eofFlagLen {
				dumpto.Write(b[:(writeLen-eofFlagLen)])
				replyLen += uint64(writeLen-eofFlagLen)
				break
			}
		} 
		
		dumpto.Write(b[:writeLen])
		
		replyLen += uint64(writeLen)
		
		if (i & logmask == 0) {
			log.Info("==================================== full sync %d", replyLen)
		}
		
	}
	
	rdbFileWriteSuccess(output, replyLen+uint64(eofFlagLen), cn)
}

func rdbFileWriteSuccess(output string, rdbsize uint64, cn *conn) {
	
	
//	rdbchan <- &rdb_file_info{output, (replyLen)}
	
	
	log.Info("redis master rdb size is %d, connect read count is %d, offset is %d", rdbsize, cn.GetReadCount(), cn.offset)
	
//	if (offset > 0) { //psync full 
//	
////	redisReplicationACK(cn, uint64(offset) + uint64(cn.GetReadCount())) // ack
//		
//		
//	} else { // sync
//		redisReplicationACK(cn, ack) 
//		cn.ResetReadCount() //BUG ? read count + ack length?
//	}
	
//	redisReplicationACK(cn, ack) 
}
