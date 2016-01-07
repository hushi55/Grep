package main

import (
	log "code.google.com/p/log4go"
	"os"
	"os/signal"
	"syscall"
)

// InitSignal register signals handler.
func InitSignal() {

	c := make(chan os.Signal, 1)

	//linux
	//signal.Notify(c, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT, syscall.SIGSTOP)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	for {
		s := <-c
		log.Info("grep4m get a signal %s", s.String())
		switch s {
		//case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGSTOP, syscall.SIGINT:
		case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			return
		case syscall.SIGHUP:
			//TODO reload
//			reload()
		default:
			return
		}
	}
}

//func reload() {
//	newConf, err := ReloadConfig()
//	if err != nil {
//		log.Error("ReloadConfig() error(%v)", err)
//		return
//	}
//	Conf = newConf
//}
