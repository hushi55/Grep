package grep4m

import (
	"flag"
	"fmt"
	. "Grep/grep4m"
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

	// init evnet and shutdown
	InitEvent()
	defer ShutdownEvent()
	
	StartMongoReplica()

	StartCheckpointDeamon()

	InitSignal()
}
