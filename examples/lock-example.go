package main

import (
	"flag"
	"fmt"
	"launchpad.net/gozk"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/marc-barry/gozk-recipes/lock"
	"github.com/marc-barry/gozk-recipes/session"

	"github.com/Sirupsen/logrus"
)

const (
	ServersFlag  = "servers"
	LockRootFlag = "lock-root"
)

var (
	Log = logrus.New()

	servers  = flag.String(ServersFlag, "localhost:2181", "The list of ZooKeeper servers.")
	lockRoot = flag.String(LockRootFlag, "/gozk-recipes", "The path to the parent leader node.")

	stopWg = sync.WaitGroup{}
	lockMu = sync.Mutex{}

	gl *lock.GlobalLock
)

func withLogging(f func()) {
	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("Recovered from panic(%+v)", r)

			Log.WithField("error", err).Panicf("Stopped with panic: %s", err.Error())
		}
	}()

	f()
}

func main() {
	flag.Parse()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		for sig := range c {
			Log.WithField("signal", sig).Infof("Signalled. Shutting down.")

			stop()

			stopWg.Done()
		}
	}()

	session, err := session.NewZkSession(*servers, time.Second*3, time.Second*3)

	if err == nil {
		Log.Infof("Session opened.")
	}

	if err != nil {
		Log.WithField("error", err).Fatalf("Couldn't establish a session with a ZooKeeper server.")
	}

	if stat, _ := session.Connection.Exists(*lockRoot); stat == nil {
		_, err = session.Connection.Create(*lockRoot, "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))

		if err != nil {
			Log.WithField("error", err).Errorf("Couldn't create root node.")
		}
	}

	stopWg.Add(1)

	go withLogging(func() {
		start(session)
	})

	stopWg.Wait()

	err = session.Connection.Delete(*lockRoot, -1)

	if err != nil {
		Log.WithField("error", err).Errorf("Couldn't delete root node.")
	}

	err = session.Close()

	if err == nil {
		Log.Infof("Session closed.")
	}

	if err != nil {
		Log.WithField("error", err).Errorf("Couldn't close session.")
	}
}

func start(session *session.ZkSession) {
	gl = lock.NewGlobalLock(session, *lockRoot)

	err := gl.Lock()

	if err == nil {
		Log.Infof("Lock obtained.")
	}

	if err != nil {
		Log.WithField("error", err).Errorf("Couldn't obtain lock.")
	}
}

func stop() {
	err := gl.Unlock()

	if err == nil {
		Log.Infof("Lock released.")
	}

	if err != nil {
		Log.WithField("error", err).Errorf("Couldn't release lock.")
	}
}
