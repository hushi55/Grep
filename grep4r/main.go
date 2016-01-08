package main

import (
	log "code.google.com/p/log4go"
	"flag"
	"fmt"
)

var (
	RetryPsync = make(chan bool)
)

func main() {

	flag.Parse()
	if err := InitConfig(); err != nil {
		fmt.Printf("init err: %s", err)
		panic(err)
	}

	log.LoadConfiguration(Conf.Log)
	defer log.Close()

	fullsync()
//		psync()

	go func() {
		for {
			select {
			case <-RetryPsync:
				psync()
			}
		}
	}()

	InitSignal()
}
