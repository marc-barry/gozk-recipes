package session

import (
	"fmt"
	"time"

	"launchpad.net/gozk"
)

var ErrNotConnected = fmt.Errorf("Unable to connect to ZooKeeper.")

type ZkSession struct {
	Connection *zookeeper.Conn
	Events     <-chan zookeeper.Event
}

func NewZkSession(servers string) (*ZkSession, error) {
	zkconn, zkevents, err := zookeeper.Dial(servers, 3*time.Second)

	if err != nil {
		return nil, err
	}

	select {
	case event := <-zkevents:
		if event.State != zookeeper.STATE_CONNECTED {
			return nil, ErrNotConnected
		}

		return &ZkSession{zkconn, zkevents}, nil
	case <-time.After(3 * time.Second):
		return nil, ErrNotConnected
	}
}
