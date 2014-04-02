package main

import (
	"flag"
	L "github.com/okcupid/logchan"
	"math/rand"
	"os"
	"os/signal"
	P "pubsub"
	"runtime"
	"runtime/pprof"
	"syscall"
	"time"
)

var configFile = flag.String("f", "/etc/conf/8lt/8lt.json", "config file")
var host = flag.String("h", "0.0.0.0", "host to bind to")
var rpcport = flag.Int("p", 49932, "port to listen on")
var streamport = flag.Int("s", 49933, "stream port to listen on")
var procs = flag.Int("c", 4, "number of procs to run")
var debug = flag.String("d", "", "debug string for pubsubd debugging channels")
var pdebug = flag.String("b", "", "debug string for pubsub debugging channels")
var mpdebug = flag.String("r", "", "debug string for msgpack RPC")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var memprofile = flag.String("memprofile", "", "write memory profile to file")

func startProfiler(file string) {
	f, err := os.Create(file)
	if err != nil {
		L.Printf(L.LOG_FATAL, "%v", err)
		os.Exit(1)
	}
	pprof.StartCPUProfile(f)
	signals := make(chan os.Signal)
	signal.Notify(signals, syscall.SIGINT)

	go func() {
		for {
			select {
			case <-signals:
				L.Printf(L.LOG_INFO, "stopping CPU profile\n")
				pprof.StopCPUProfile()
				os.Exit(0)
			case <-time.After(1 * time.Second):
			}
		}
	}()
}

func main() {
	rand.Seed(time.Now().UnixNano())
	flag.Parse()
	runtime.GOMAXPROCS(*procs)
	server := NewServer(*configFile, *host, *rpcport, *host, *streamport)

	L.SetChannelsEasy("pubsubd", *debug, false)
	P.Logger.SetChannelsEasy("pubsub", *pdebug, false)
	server.Mps.Logger.SetChannelsEasy("mpack", *mpdebug, false)

	if *cpuprofile != "" {
		startProfiler(*cpuprofile)
	}

	server.Run()
}