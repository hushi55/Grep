package main

import (
	"os"
	"time"
	"strconv"
	"strings"
	log "code.google.com/p/log4go"
)

type redisRepliInfo struct {
	runid string
	offset int64
}

var (
	cpLog        = log.NewLogger()
	CP_FILE_NAME = "checkpoint"
	initFlag	 = false
	cplen        = len("[2016/01/09 19:37:39 CST] [INFO] (main.writecp:81) 09853e6ab47ec0ce0a585f0fd55118f1a4671ddd 6231782484598587392")
)

func init() {
	if !initFlag {
		cpLog.AddFilter("log", log.FINE, log.NewFileLogWriter(CP_FILE_NAME, false))
		initFlag = true
	}
}

type deamon_timer struct {
	timer *time.Timer
	d     time.Duration
}

func newDeamonTimer(d time.Duration) *deamon_timer {
	dtimer := new(deamon_timer)
	dtimer.timer = time.NewTimer(d)
	dtimer.d = d

	return dtimer
}

func (this *deamon_timer) reset() {
	this.timer.Reset(this.d)
}

func initRedisRepilcationInfo() (string, int64) {
	fname := CP_FILE_NAME
	file, err := os.Open(fname)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	buf := make([]byte, cplen)
	stat, err := os.Stat(fname)
	start := stat.Size() - int64(cplen)
	_, err = file.ReadAt(buf, start)

	if err == nil {

		items := strings.Split(string(buf), " ")

		if items != nil {
			l := len(items)
			if (l > 1) {
				v := items[l-1] //last item
				offset, err := strconv.ParseInt(strings.Trim(v, "\n"), 10, 64)
				if err == nil {
					return items[l-2], offset
				}
			}
		}
	}

	return "?", -1
}

func writecp(cp *redisRepliInfo, msg string) {
	log.Info("write check point to file by reason of %s ...", msg)
	cpLog.Info("%s	%d", cp.runid, cp.offset)
}
