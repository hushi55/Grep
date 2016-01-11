package main

import (
	log "code.google.com/p/log4go"
	"flag"
	"fmt"
)

func main() {

	flag.Parse()
	if err := InitConfig(); err != nil {
		fmt.Printf("init err: %s", err)
		panic(err)
	}

	log.LoadConfiguration(Conf.Log)
	defer log.Close()

	SyncDaemon()

	InitSignal()
}
