package session

import (
	"fmt"
	"time"

	"launchpad.net/gozk"
)

type ZkSession struct {
	Connection *zookeeper.Conn
	Events     <-chan zookeeper.Event
}

func errNotConnected(err error) error {
	if err != nil {
		return fmt.Errorf("Unable to connect to ZooKeeper: %s", err.Error())
	}
	return fmt.Errorf("Unable to connect to ZooKeeper.")
}

func NewZkSession(servers string, dialTimeout time.Duration, recvTimeout time.Duration) (*ZkSession, error) {
	zkconn, zkevents, err := zookeeper.Dial(servers, recvTimeout)

	if err != nil {
		return nil, err
	}

	for {
		select {
		case event := <-zkevents:
			// Only the STATE_CONNECTED session event is valid when establishing a new session.
			// Any other state is ignored.
			if event.State == zookeeper.STATE_CONNECTED {
				return &ZkSession{zkconn, zkevents}, nil
			}
		case <-time.After(dialTimeout):
			return nil, errNotConnected(zkconn.Close())
		}
	}

	return nil, errNotConnected(zkconn.Close())
}
