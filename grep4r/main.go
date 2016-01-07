package main

import (
	"flag"
	"fmt"
	log "code.google.com/p/log4go"
)

func main() {
	
	flag.Parse()
	if err := InitConfig(); err != nil {
		fmt.Printf("init err: %s", err);
		panic(err)
	}
	
	log.LoadConfiguration(Conf.Log)
	defer log.Close()
	
	fullsync()

	InitSignal()
}
