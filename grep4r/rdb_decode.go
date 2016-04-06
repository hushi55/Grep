// Copyright 2014 Wandoujia Inc. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	
	rdb "Grep/grep4r/rdb"
	log "code.google.com/p/log4go"
//	"github.com/wandoulabs/redis-port/pkg/libs/atomic2"
)

var (
	keyLog        = log.NewLogger()
	keys		 =  map[string]struct{}{}
)

func init() {
	keyLog.AddFilter("log", log.FINE, log.NewFileLogWriter("keys-test", false))
}

const (
	ReaderBufferSize = 1024 * 1024 * 32
	WriterBufferSize = 1024 * 1024 * 8
)

func parserRDBFile(input string, rdbsize uint64) {
	
	var readin io.ReadCloser
	var nsize int64
	wait := make(chan bool)
	if input != "/dev/stdin" {
		readin, nsize = openReadFile(input)
		defer readin.Close()
		
		log.Info("parser rdb file, master network rdb size is %d, local file rdb size is %d", rdbsize, nsize)
		
	} else {
		readin, nsize = os.Stdin, 0
	}


	reader := bufio.NewReaderSize(readin, ReaderBufferSize)
	
	pipe := make(chan *rdb.BinEntry, 1024)
	go func() {
		defer close(pipe)
		l := rdb.NewLoader(reader)
		if err := l.Header(); err != nil {
			log.Error("parse rdb header error: %s", err)
		}
		for {
			if entry, err := l.NextBinEntry(); err != nil {
				log.Error("parse rdb entry error: %s", err)
			} else {
				if entry != nil {
					pipe <- entry
				} else {
					if err := l.Footer(); err != nil {
						log.Error("parse rdb checksum error: %s", err)
					}
					return
				}
			}
		}
		
		wait <- false
		
	}()
	
	
	go decoder(pipe)
	
	select {
		case <- wait:
		log.Info("parser rdb file successful")
	}
	
	log.Info("nsize is %d", nsize)
}


func decoder(ipipe <-chan *rdb.BinEntry) {
	toText := func(p []byte) string {
		var b bytes.Buffer
		for _, c := range p {
			switch {
			case c >= '#' && c <= '~':
				b.WriteByte(c)
			default:
				b.WriteByte('.')
			}
		}
		return b.String()
	}
	toBase64 := func(p []byte) string {
		return base64.StdEncoding.EncodeToString(p)
	}
	toJson := func(o interface{}) string {
		b, err := json.Marshal(o)
		if err != nil {
			log.Error("encode to json failed", err)
		}
		return string(b)
	}
	for e := range ipipe {
		o, err := rdb.DecodeDump(e.Value)
		if err != nil {
			log.Error("decode failed : %s", err)
		}
		var b bytes.Buffer
		switch obj := o.(type) {
		default:
			log.Error("unknown object %v", o)
		case rdb.String:
			o := &struct {
				DB       uint32 `json:"db"`
				Type     string `json:"type"`
				ExpireAt uint64 `json:"expireat"`
				Key      string `json:"key"`
				Key64    string `json:"key64"`
				Value64  string `json:"value64"`
			}{
				e.DB, "string", e.ExpireAt, toText(e.Key), toBase64(e.Key),
				toBase64(obj),
			}
			fmt.Fprintf(&b, "%s\n", toJson(o))
		case rdb.List:
			for i, ele := range obj {
				o := &struct {
					DB       uint32 `json:"db"`
					Type     string `json:"type"`
					ExpireAt uint64 `json:"expireat"`
					Key      string `json:"key"`
					Key64    string `json:"key64"`
					Index    int    `json:"index"`
					Value64  string `json:"value64"`
				}{
					e.DB, "list", e.ExpireAt, toText(e.Key), toBase64(e.Key),
					i, toBase64(ele),
				}
				fmt.Fprintf(&b, "%s\n", toJson(o))
			}
		case rdb.Hash:
			for _, ele := range obj {
				o := &struct {
					DB       uint32 `json:"db"`
					Type     string `json:"type"`
					ExpireAt uint64 `json:"expireat"`
					Key      string `json:"key"`
					Key64    string `json:"key64"`
					Field    string `json:"field"`
					Field64  string `json:"field64"`
					Value64  string `json:"value64"`
				}{
					e.DB, "hash", e.ExpireAt, toText(e.Key), toBase64(e.Key),
					toText(ele.Field), toBase64(ele.Field), toBase64(ele.Value),
				}
				fmt.Fprintf(&b, "%s\n", toJson(o))
			}
		case rdb.Set:
			for _, mem := range obj {
				o := &struct {
					DB       uint32 `json:"db"`
					Type     string `json:"type"`
					ExpireAt uint64 `json:"expireat"`
					Key      string `json:"key"`
					Key64    string `json:"key64"`
					Member   string `json:"member"`
					Member64 string `json:"member64"`
				}{
					e.DB, "set", e.ExpireAt, toText(e.Key), toBase64(e.Key),
					toText(mem), toBase64(mem),
				}
				fmt.Fprintf(&b, "%s\n", toJson(o))
			}
		case rdb.ZSet:
			for _, ele := range obj {
				o := &struct {
					DB       uint32  `json:"db"`
					Type     string  `json:"type"`
					ExpireAt uint64  `json:"expireat"`
					Key      string  `json:"key"`
					Key64    string  `json:"key64"`
					Member   string  `json:"member"`
					Member64 string  `json:"member64"`
					Score    float64 `json:"score"`
				}{
					e.DB, "zset", e.ExpireAt, toText(e.Key), toBase64(e.Key),
					toText(ele.Member), toBase64(ele.Member), ele.Score,
				}
				fmt.Fprintf(&b, "%s\n", toJson(o))
				
				if strings.HasPrefix(o.Key, "00000:z_group_msg")  {
					_, exists := keys[o.Key]
					if !exists {
						keys[o.Key] = struct{}{}
						keyLog.Info(o.Key)
					} 
				}
			}
		}
//		opipe <- b.String()
		fmt.Println(b.String())
	}
}
