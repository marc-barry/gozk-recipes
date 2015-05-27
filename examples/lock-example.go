package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/Shopify/gozk-recipes/lock"
	"github.com/Shopify/gozk-recipes/session"

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

	gl       *lock.GlobalLock
	locked   bool = false
	lockedMu      = sync.Mutex{}
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

	stopWg.Add(1)

	sess, events, err := session.NewZKSession(*servers, time.Second*1)
	if err != nil {
		Log.WithField("error", err).Fatalf("Couldn't establish a session with a ZooKeeper server.")
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		var once sync.Once

		for sig := range c {
			Log.WithField("signal", sig).Infof("Signalled. Shutting down.")

			once.Do(func() {
				stop(sess)
			})
		}
	}()

	Log.Info("Session created.")

	go withLogging(func() {
		start(sess, events)
	})

	stopWg.Wait()
}

func start(sess *session.ZKSession, events <-chan session.ZKSessionEvent) {
	var err error

	gl, err = lock.NewGlobalLock(sess, *lockRoot, "")
	if err != nil {
		Log.WithField("error", err).Errorf("Couldn't create lock.")
		return
	}

	go func() {
		getLock()
		for {
			select {
			case event := <-events:
				switch event {
				case session.SessionClosed:
					Log.WithField("event", "closed").Infof("The session was closed.")
				case session.SessionDisconnected:
					Log.WithField("event", "disconnected").Infof("The session was disconnected.")
				case session.SessionReconnected:
					Log.WithField("event", "reconnected").Infof("The session was reconnected.")
					lockedMu.Lock()
					Log.WithField("event", "reconnected").Infof("Previous lock state: %v", locked)
					lockedMu.Unlock()
					getLock()
				case session.SessionExpiredReconnected:
					Log.WithField("event", "expired reconnected").Infof("The session was expired and reconnected.")
					lockedMu.Lock()
					Log.WithField("event", "expired reconnected").Infof("Previous lock state: %v", locked)
					lockedMu.Unlock()
					getLock()
				case session.SessionFailed:
					Log.WithField("event", "failed session").Errorf("The session failed.")
					stop(sess)
				}
			}
		}
	}()
}

func getLock() {
	defer lockedMu.Unlock()
	lockedMu.Lock()

	Log.Infof("Trying to lock.")

	locked = false

	err := gl.Lock()
	if err == nil {
		locked = true
		Log.Infof("Lock obtained.")
	}
	if err != nil {
		Log.WithField("error", err).Errorf("Couldn't obtain lock.")
	}
}

func stop(sess *session.ZKSession) {
	defer stopWg.Done()

	if gl != nil {
		err := gl.Unlock()
		if err == nil {
			Log.Infof("Lock released.")
		}
		if err != nil {
			Log.WithField("error", err).Errorf("Couldn't release lock.")
		}

		err = gl.Destroy()
		if err != nil {
			Log.WithField("error", err).Errorf("Couldn't destroy lock.")
		}
	}

	err := sess.Close()
	if err == nil {
		Log.Infof("Session closed.")
	}
	if err != nil {
		Log.WithField("error", err).Errorf("Couldn't close session.")
	}
}
