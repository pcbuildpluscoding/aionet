package test

import (
	"fmt"
	"testing"
	"time"

	aio "github.com/pcbuildpluscoding/aionet"
)

// ===========================================================================
func tc_netdb4(t *testing.T, params Result, args ...any) Result {
	logger.Debugf("$$$$$$$$$$$ running tc_netdb4 $$$$$$$$$$$$")
	err := func() error {
		l, err := aio.NewHdpListener("udp", *hdpnetAddr, 16)
		if err != nil {
			return err
		}
		d, err := aio.NewHdpDialer()
		if err != nil {
			return err
		}

		errCh := make(chan error, 1)
		readyCh := [2]chan bool{make(chan bool, 1), make(chan bool, 1)}
		go listen4(l, readyCh, errCh, &params)
		go dial4(d, readyCh, errCh, &params)
		logger.Debugf("tc_netdb4 is running ...")
		dura := time.Duration(30) * time.Second
		count := 0
		for count < 2 {
			select {
			case <-time.After(dura):
				return fmt.Errorf("tc_netdb timedout !!")
			case err := <-errCh:
				if err != nil {
					return fmt.Errorf("tc_netdb got a listen or dial error : %v", err)
				}
			}
			count++
		}

		logger.Debugf("tc_netdb is complete")
		return nil
	}()
	return params.With(err)
}

// ===========================================================================
func dial4(d1 *aio.HdpDialer, readyCh [2]chan bool, errCh chan error, params *Result) {
	logger.Debugf("%s is running ...", d1.Cid())
	conn, err := d1.Dial("udp", *hdpnetAddr)
	if err != nil {
		errCh <- err
		return
	}
	logger.Debugf("@@@@@@@@@@@@@@@ got a conn from dialer.Dial @@@@@@@@@@@@@@@@@@@")
	readyCh[R] <- true
	<-readyCh[W]
	n, err := conn.Write([]byte("TEST1 : THIS CODE MUST READ AND WRITE"))
	logger.Debugf("%s got conn.Write result, num bytes written, error : %d, %v", conn.Cid(), n, err)
	b := make([]byte, 37)
	n, err = conn.Read(b)
	logger.Debugf("%s got conn.Read result, num bytes, error, frame : %d, %v, %s", conn.Cid(), n, err, b)
	<-time.After(500 * time.Millisecond)
	params.Add(":data", "dialConn", conn)
	errCh <- err
}

// ===========================================================================
func listen4(l1 *aio.HdpListener, readyCh [2]chan bool, errCh chan error, params *Result) {
	logger.Debugf("%s is running ...", l1.Cid())
	conn, err := l1.Accept()
	if err != nil {
		errCh <- err
		return
	}
	logger.Debugf("@@@@@@@@@@@@@@@ got a conn from listener.Accept @@@@@@@@@@@@@@@@@@@")
	// word := "TEST1 : THIS CODE MUST READ AND WRITE"
	readyCh[W] <- true
	<-readyCh[R]
	b := make([]byte, 37)
	n, err := conn.Read(b)
	logger.Debugf("%s got conn.Read result, num bytes, error, frame : %d, %v, %s", conn.Cid(), n, err, b)
	n, err = conn.Write([]byte("TEST1 : THIS CODE MUST READ AND WRITE"))
	logger.Debugf("%s got conn.Write result, num bytes written, error : %d, %v", conn.Cid(), n, err)
	params.Add(":data", "acceptConn", conn)
	errCh <- err
}

// ===========================================================================
func tc_netdb5(t *testing.T, params Result, args ...any) Result {
	logger.Debugf("$$$$$$$$$$$ running tc_netdb5 $$$$$$$$$$$$")
	err := func() error {
		l, err := aio.NewHdpListener("udp", *hdpnetAddr, 16)
		if err != nil {
			return err
		}
		d, err := aio.NewHdpDialer()
		if err != nil {
			return err
		}

		errCh := make(chan error, 1)
		readyCh := [2]chan bool{make(chan bool, 1), make(chan bool, 1)}
		go listen5(l, readyCh, errCh, &params)
		go dial5(d, readyCh, errCh, &params)
		logger.Debugf("tc_netdb5 is running ...")
		dura := time.Duration(30) * time.Second
		count := 0
		for count < 2 {
			select {
			case <-time.After(dura):
				return fmt.Errorf("tc_netdb timedout !!")
			case err := <-errCh:
				if err != nil {
					return fmt.Errorf("tc_netdb got a listen or dial error : %v", err)
				}
			}
			count++
		}

		logger.Debugf("tc_netdb is complete")
		return nil
	}()
	return params.With(err)
}

// ===========================================================================
func dial5(d1 *aio.HdpDialer, readyCh [2]chan bool, errCh chan error, params *Result) {
	logger.Debugf("%s is running ...", d1.Cid())
	conn, err := d1.Dial("udp", *hdpnetAddr)
	if err != nil {
		errCh <- err
		return
	}
	logger.Debugf("@@@@@@@@@@@@@@@ got a conn from dialer.Dial @@@@@@@@@@@@@@@@@@@")
	readyCh[R] <- true
	<-readyCh[W]
	<-time.After(500 * time.Millisecond)
	err = conn.Close()
	logger.Debugf("%s got conn.Close result, error : %v", conn.Cid(), err)
	params.Add(":data", "dialConn", conn)
	errCh <- err
}

// ===========================================================================
func listen5(l1 *aio.HdpListener, readyCh [2]chan bool, errCh chan error, params *Result) {
	logger.Debugf("%s is running ...", l1.Cid())
	conn, err := l1.Accept()
	if err != nil {
		errCh <- err
		return
	}
	logger.Debugf("@@@@@@@@@@@@@@@ got a conn from listener.Accept @@@@@@@@@@@@@@@@@@@")
	// word := "TEST1 : THIS CODE MUST READ AND WRITE"
	readyCh[W] <- true
	<-readyCh[R]
	b := make([]byte, 7)
	n, err := conn.Read(b)
	logger.Debugf("%s got conn.Read result, num bytes, error, frame : %d, %v, %s", conn.Cid(), n, err, b)
	params.Add(":data", "acceptConn", conn)
	errCh <- err
}
