package main

import (
	"bufio"
	"net"
	"time"
	"github.com/hushi55/golib/lang/atomic"
)

const defaultBufSize = 4096

var (
	zeroTime = time.Time{}
)

type conn struct {
	netcn net.Conn
	rd    *bufio.Reader
	buf   []byte
	
	rc	 atomic.Int64
	runid 	string
	offset 	int64

	usedAt       time.Time
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

//func (this *conn) reset() {
//	buffer := make([]byte, 4096)
//	newcon := &conn{
//		netcn: this.netcn,
//		buf: buffer,
//	}
//	rd := bufio.NewReader(newcon);
//	this.rd.Reset(rd)
//}

//func newConnDialer(opt *Options) func() (*conn, error) {
//	dialer := opt.getDialer()
//	return func() (*conn, error) {
//		netcn, err := dialer()
//		if err != nil {
//			return nil, err
//		}
//		cn := &conn{
//			netcn: netcn,
//			buf:   make([]byte, defaultBufSize),
//		}
//		cn.rd = bufio.NewReader(cn)
//		return cn, cn.init(opt)
//	}
//}

//func (cn *conn) init(opt *Options) error {
//	if opt.Password == "" && opt.DB == 0 {
//		return nil
//	}
//
//	// Temp client for Auth and Select.
//	client := newClient(opt, newSingleConnPool(cn))
//
//	if opt.Password != "" {
//		if err := client.Auth(opt.Password).Err(); err != nil {
//			return err
//		}
//	}
//
//	if opt.DB > 0 {
//		if err := client.Select(opt.DB).Err(); err != nil {
//			return err
//		}
//	}
//
//	return nil
//}

func (cn *conn) writeCmds(cmds ...Cmder) error {
	buf := cn.buf[:0]
	for _, cmd := range cmds {
		var err error
		buf, err = appendArgs(buf, cmd.args())
		if err != nil {
			return err
		}
	}

	_, err := cn.Write(buf)
	return err
}

func (cn *conn) Read(b []byte) (int, error) {
	if cn.ReadTimeout != 0 {
		cn.netcn.SetReadDeadline(time.Now().Add(cn.ReadTimeout))
	} else {
		cn.netcn.SetReadDeadline(zeroTime)
	}
	
	n, err := cn.netcn.Read(b);
	
	cn.rc.Add(int64(n))
	
	return n, err
}

func (cn *conn) Write(b []byte) (int, error) {
	if cn.WriteTimeout != 0 {
		cn.netcn.SetWriteDeadline(time.Now().Add(cn.WriteTimeout))
	} else {
		cn.netcn.SetWriteDeadline(zeroTime)
	}
	return cn.netcn.Write(b)
}

func (cn *conn) RemoteAddr() net.Addr {
	return cn.netcn.RemoteAddr()
}

func (cn *conn) ResetReadCount() {
	cn.rc.Set(0);
}

func (cn *conn) GetReadCount() int64 {
	return cn.rc.Get()
}

func (cn *conn) Close() error {
	return cn.netcn.Close()
}

func isSameSlice(s1, s2 []byte) bool {
	return len(s1) > 0 && len(s2) > 0 && &s1[0] == &s2[0]
}

func (cn *conn) copyBuf(b []byte) []byte {
	if isSameSlice(b, cn.buf) {
		new := make([]byte, len(b))
		copy(new, b)
		return new
	}
	return b
}
