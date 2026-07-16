package test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	aio "github.com/pcbuildpluscoding/aionet"
	"github.com/pcbuildpluscoding/aionet/dtype"
	"github.com/pcbuildpluscoding/aionet/epoller"
)

// =================================================================
func TestHdp(t *testing.T) {
	err := setLogger()
	if err != nil {
		t.Fatalf("set logger error : %v", err)
	}
	logger.Debugf("$$$$$$$$$$$$ running testHdp $$$$$$$$$$$$$")
	aio.SetLogger(logger)
	dtype.SetLogger(logger)
	epoller.SetLogger(logger)

	logger.Debugf("$$$$$$$$$$ hdpnetAddr: %s", *hdpnetAddr)
	logger.Debugf("$$$$$$$$$$ testcases : %s", *testcases)
	logger.Debugf("$$$$$$$$$$ fifoDir: %s", *fifoDir)

	os.Setenv("NETIO_FIFO_DIR", *fifoDir)

	logger.Debugf("TestHdp1 startTest is complete !!! ...")

	t.Run("HdpTesting", func(t *testing.T) {
		tcslice, err := getTestbook1()
		if err != nil {
			t.Fatal(err)
		}
		result := Result{}
		for _, tc := range tcslice {
			name := tc.String("name")
			logger.Debugf("TestHdp1 next testcase : %v", tc)
			tf, _ := tc[name].(func(*testing.T, Result, ...any) Result)
			if tf == nil {
				t.Fatalf("testcase |%s| is undefined", name)
			}
			result = tf(t, result)
			logger.Debugf("got result : %v", result)
		}
	})
	dura := time.Duration(3) * time.Second
	<-time.After(dura)
	epoller.Stop()
	// cleanup("*.accept", "*.dial", "*.conn")
}

// =================================================================
func getTestbook1() ([]Testcase, error) {
	x := strings.Split(*testcases, ",")
	y := make([]Testcase, len(x))
	for i, tc := range x {
		switch strings.TrimSpace(tc) {
		case "tc_netdb1":
			y[i] = Testcase{"tc_netdb1": tc_netdb1, "name": "tc_netdb1"}
		case "tc_netdb2":
			y[i] = Testcase{"tc_netdb2": tc_netdb2, "name": "tc_netdb2"}
		case "tc_netdb3":
			y[i] = Testcase{"tc_netdb3": tc_netdb3, "name": "tc_netdb3"}
		case "tc_netdb4":
			y[i] = Testcase{"tc_netdb4": tc_netdb4, "name": "tc_netdb4"}
		case "tc_netdb5":
			y[i] = Testcase{"tc_netdb5": tc_netdb5, "name": "tc_netdb5"}
		default:
			return nil, fmt.Errorf("unknown testcase name : |%s|", tc)
		}
	}
	return y, nil
}

// =================================================================
func cleanup(args ...string) {
	logger.Debugf("got fifo directory : %s", *fifoDir)
	for _, fglob := range args {
		removeFiles(*fifoDir + "/" + fglob)
	}
}

// =================================================================
func removeFiles(fname string) {
	logger.Debugf("got glob files for removal : %s", fname)
	files, err := filepath.Glob(fname)
	if err != nil {
		logger.Errorf("got filepath.Glob error : %v", err)
		return
	}
	for _, f := range files {
		logger.Debugf("got file for removal : %s", f)
		if err := os.Remove(f); err != nil {
			logger.Errorf("got file remove error : %v", err)
		}
	}
}
