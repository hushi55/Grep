package main

import (
	log "code.google.com/p/log4go"
)

func reload() {
	newConf, err := ReloadConfig()
	if err != nil {
		log.Error("ReloadConfig() error(%v)", err)
		return
	}
	Conf = newConf
}
