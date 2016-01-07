package grep4m

import (
	"os"
	"fmt"
	"time"
	"strconv"
	"strings"
	log "code.google.com/p/log4go"
)

var (
	checkpoint   = make(chan int64, 1)
	cpLog        = log.NewLogger()
	CP_FILE_NAME = "checkpoint"
	cplen        = len("[2015/12/24 17:38:47 CST] [INFO] (mongodb-driver/grep4m.writecp:106) 6231782484598587392")
)

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

// checkpoing deamon go
func StartCheckpointDeamon() {

	cpLog.AddFilter("log", log.FINE, log.NewFileLogWriter(CP_FILE_NAME, false))

//	t := newDeamonTimer(Conf.CheckPointTimeout)

	go func() {
		for {
			select {
			case cp := <-checkpoint:

				writecp(cp, fmt.Sprintf("exceed threshold %d", Conf.CheckPointThreshold))

//				t.reset()

			case <-time.After(Conf.CheckPointTimeout):
				seconds := int32(time.Now().Unix())
				writecp(int64(seconds)<<32, fmt.Sprintf("timeout %d millisecond", Conf.CheckPointTimeout/(1000*1000)))

//				t.reset()
			}
		}
	}()
}

func initCheckpoint() int64 {
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
			v := items[l-1] //last item
			cp, err := strconv.ParseInt(strings.Trim(v, "\n"), 10, 64)
			if err == nil {
				return cp
			}
		}
	}

	return int64(int64(time.Now().Unix()) << 32)
}

func writecp(cp int64, msg string) {
	log.Info("write check point to file by reason of %s ...", msg)
	cpLog.Info("%d", cp)
}
