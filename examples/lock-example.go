package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/Shopify/gozk-recipes/lock"
	"github.com/Shopify/gozk-recipes/session"
)

const (
	ServersFlag  = "servers"
	LockRootFlag = "lock-root"
)

var (
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

			log.Fatalf("stopped with error %s", err)
		}
	}()

	f()
}

func main() {
	flag.Parse()

	stopWg.Add(1)

	sess, err := session.NewZKSession(*servers, time.Second*1, log.Default())
	if err != nil {
		log.Fatalf("Couldn't establish a session with a ZooKeeper server. %s", err)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		var once sync.Once

		for sig := range c {
			log.Printf("Signalled. Shutting down. Signal: %v", sig)

			once.Do(func() {
				stop(sess)
			})
		}
	}()

	log.Printf("Session created.")

	go withLogging(func() {
		start(sess)
	})

	stopWg.Wait()
}

func start(sess *session.ZKSession) {
	var err error

	gl, err = lock.NewGlobalLock(sess, *lockRoot, "")
	if err != nil {
		log.Fatalf("Couldn't create lock. %s", err)
		return
	}

	events := make(chan session.ZKSessionEvent)
	sess.Subscribe(events)

	go func() {
		getLock()
		for {
			select {
			case event := <-events:
				switch event {
				case session.SessionClosed:
					log.Printf("The session was closed.")
				case session.SessionDisconnected:
					log.Printf("The session was disconnected.")
				case session.SessionReconnected:
					log.Printf("The session was reconnected.")
					lockedMu.Lock()
					log.Printf("Previous lock state: %v", locked)
					lockedMu.Unlock()
					getLock()
				case session.SessionExpiredReconnected:
					log.Printf("The session was expired and reconnected.")
					lockedMu.Lock()
					log.Printf("Previous lock state: %v", locked)
					lockedMu.Unlock()
					getLock()
				case session.SessionFailed:
					log.Printf("The session failed.")
					stop(sess)
				}
			}
		}
	}()
}

func getLock() {
	defer lockedMu.Unlock()
	lockedMu.Lock()

	log.Printf("Trying to lock.")

	locked = false

	err := gl.Lock()
	if err == nil {
		locked = true
		log.Printf("Lock obtained.")
	}
	if err != nil {
		log.Fatalf("Couldn't obtain lock. %s", err)
	}
}

func stop(sess *session.ZKSession) {
	defer stopWg.Done()

	if gl != nil {
		err := gl.Unlock()
		if err == nil {
			log.Printf("Lock released.")
		}
		if err != nil {
			log.Printf("Couldn't release lock. %s", err)
		}

		err = gl.Destroy()
		if err != nil {
			log.Printf("Couldn't destroy lock. %s", err)
		}
	}

	err := sess.Close()
	if err == nil {
		log.Print("Session closed.")
	}
	if err != nil {
		log.Printf("Couldn't close session. %s", err)
	}
}
