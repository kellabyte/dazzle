package main

import (
	"flag"
	"fmt"
	"github.com/kellabyte/dazzle/database"
	"log"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"syscall"
)

var loggingVerbose bool
var loggingTrace bool
var loggingDebug bool
var name string
var host string
var port int
var path string
var leaderAddress string
var profiling bool

func init() {
	flag.BoolVar(&loggingVerbose, "verbose", false, "verbose logging")
	flag.BoolVar(&loggingTrace, "trace", false, "Raft trace debugging")
	flag.BoolVar(&loggingDebug, "debug", false, "Raft debugging")
	flag.StringVar(&name, "name", "node", "node name")
	flag.StringVar(&host, "host", "localhost", "hostname")
	flag.IntVar(&port, "port", 4001, "port")
	flag.StringVar(&path, "path", "", "node")
	flag.StringVar(&leaderAddress, "leader", "", "host:port of leader to join")
	flag.BoolVar(&profiling, "profile", false, "Enable profiling")
}

func main() {
	log.SetFlags(0)
	flag.Parse()

	if profiling {
		fname := "./cpu.pprof"

		os.Remove(fname)
		f, err := os.Create(fname)
		if err != nil {
			fmt.Printf("couldn't create file: %v, err: %v", fname, err)
			return
		}
		pprof.StartCPUProfile(f)
	}

	if path == "" {
		path = "./" + name
	}

	log.Printf("\nname: %s\nhost: %s\nport: %d\npath: %s\nleader: %s",
		name,
		host,
		port,
		path,
		leaderAddress)

	database.SetLogging(loggingVerbose, loggingTrace, loggingDebug, host, port)

	runtime.GOMAXPROCS(runtime.NumCPU())
	go signalCatcher()

	db, err := database.NewDatabase(name, host, port, path, leaderAddress)
	if err != nil {
		fmt.Println("Error opening database")
	}

	db.ListenAndServe("0.0.0.0", port)
}

func signalCatcher() {
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT)
	<-ch
	log.Println("CTRL-C; exiting")
	pprof.StopCPUProfile()
	os.Exit(0)
}
