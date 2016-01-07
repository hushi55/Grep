package main

import (
	"flag"
	"time"
	"runtime"
	"github.com/Terry-Mao/goconf"
)

var (
	gconf    *goconf.Config
	Conf     *Config
	confFile string
)

func init() {
	flag.StringVar(&confFile, "c", "./grep4r/grep4r.conf", " set relipcation for redis config file path")
}

type Config struct {
	// base section
	PidFile   string   `goconf:"base:pidfile"`
	Dir       string   `goconf:"base:dir"`
	Log       string   `goconf:"base:log"`
	MaxProc   int      `goconf:"base:maxproc"`
	PprofBind []string `goconf:"base:pprof.bind:,"`
	StatBind  []string `goconf:"base:stat.bind:,"`
	Debug     bool     `goconf:"base:debug"`
	
	CheckPointThreshold	 	uint64 			`goconf:"base:threshold"`
	CheckPointTimeout 		time.Duration 	`goconf:"base:timeout:time"`

	// events	
	TopicCollections	 	[]string		`goconf:"events:eventsources"`
	Updatefields	 		[]string		`goconf:"events:updatefields"`
	
	// redis
	RedisMasterIP         		string 	 `goconf:"redis:ip"`
	RedisMasterPort	        	string 	 `goconf:"redis:port"`
	RedisMasterPasswd	        string 	 `goconf:"redis:passwd"`
	RedisRDBFilePath	        string 	 `goconf:"redis:rdbpath"`
	
	// amqp
	AmqpIp         		string 	 `goconf:"amqp:ip"`
	AmqpPort	        string 	 `goconf:"amqp:port"`
	AmqpUser	        string 	 `goconf:"amqp:user"`
	AmqpPasswd	        string 	 `goconf:"amqp:passwd"`
}

func NewConfig() *Config {
	return &Config{
		// base section
		PidFile:   "/tmp/mongdb_replica.pid",
		Dir:       "./",
		Log:       "./log/xml",
		MaxProc:   runtime.NumCPU(),
		PprofBind: []string{"localhost:6971"},
		StatBind:  []string{"localhost:6972"},
		Debug:     true,
		
		CheckPointThreshold:		 	1024*16,
		CheckPointTimeout:		 		time.Minute*5,
		
		// redis
		RedisMasterIP:							"127.0.0.1",
		RedisMasterPort:	       				"6379",
		RedisMasterPasswd:	       				"",
		RedisRDBFilePath:	       				"./rdb.dump",
		
		// amqp
		AmqpIp:         				"localhost",
		AmqpPort:	    	    		"5672",
		AmqpUser:		        		"guest",
		AmqpPasswd:		        		"guest",
	}
}

// InitConfig init the global config.
func InitConfig() (err error) {
	
	defer func(){
		Conf.CheckPointThreshold = power2(Conf.CheckPointThreshold);
	}()
	
	Conf = NewConfig()
	gconf = goconf.New()
	if err = gconf.Parse(confFile); err != nil {
		return err
	}
	if err := gconf.Unmarshal(Conf); err != nil {
		return err
	}
	return nil
}

func ReloadConfig() (*Config, error) {
	conf := NewConfig()
	ngconf, err := gconf.Reload()
	if err != nil {
		return nil, err
	}
	if err := ngconf.Unmarshal(conf); err != nil {
		return nil, err
	}
	gconf = ngconf
	
	conf.CheckPointThreshold = power2(conf.CheckPointThreshold);
	
	return conf, nil
}

func power2(num uint64) uint64 {
	
	if num <=0 { //default value
		// 1024 * 16
		return 1 << 14
	}
	
	// 2^N
	if num&(num-1) != 0 {
		for num&(num-1) != 0 {
			num &= (num - 1)
		}
		num = num << 1
	} 
	
	return num
}
