package test

import (
	"flag"

	"github.com/google/uuid"
	"github.com/pcbuildpluscoding/logroll"
)

const (
	R = 0
	W = 1
)

var logger = logroll.New()

var testcases = flag.String("testcases", "", "comma separated list of testcases to run")
var piserverAddr = flag.String("piserverAddr", uuid.New().String(), "pollinit server pipename")
var hdpnetAddr = flag.String("hdpnetAddr", uuid.New().String(), "HdpListener pipename")
var fifoDir = flag.String("fifoDir", "", "output fifo file directory")
var testFunc1 = flag.String("testFunc1", "TestHdp2", "netdb testing func")

// ===========================================================================
func setLogger() error {

	logPath := "./testing.log"

	var err error
	logger, err = logroll.WithFile(logPath, logroll.DebugLevel)
	return err
}

// ===========================================================================
func NewResult(args ...any) Result {
	r := Result{}
	return r.With(args...)
}
