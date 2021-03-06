package main

import (
	"flag"
	"fmt"
	log "code.google.com/p/log4go"
	"github.com/hushi55/golib/signal"
)

func main() {

	flag.Parse()
	if err := InitConfig(); err != nil {
		fmt.Printf("init err: %s", err)
		panic(err)
	}

	log.LoadConfiguration(Conf.Log)
	defer log.Close()
	
	// init evnet and shutdown
	InitEvent()
	defer ShutdownEvent()

	SyncDaemon()
	
	signal.InitSignal(reload)
}
