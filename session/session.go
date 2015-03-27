package session

import (
	"fmt"
	"sync"
	"time"

	"launchpad.net/gozk"
)

var (
	ErrZkSessionNotConnected       = fmt.Errorf("Unable to connect to ZooKeeper.")
	ErrZkSessionTimeout            = fmt.Errorf("Session connection timeout expired.")
	ErrZkSessionExpired            = fmt.Errorf("Session expired.")
	ErrZkSessionEventChannelClosed = fmt.Errorf("Session event channel closed.")
)

type ZkSession struct {
	servers     string
	dialTimeout time.Duration
	recvTimeout time.Duration
	Connection  *zookeeper.Conn
	events      <-chan zookeeper.Event
	mu          sync.Mutex
}

func NewZkSession(servers string, dialTimeout time.Duration, recvTimeout time.Duration) (*ZkSession, error) {
	zkconn, zkevents, err := zookeeper.Dial(servers, recvTimeout)

	if err != nil {
		return nil, err
	}

	s := &ZkSession{servers: servers, dialTimeout: dialTimeout, recvTimeout: recvTimeout, mu: sync.Mutex{}}

	if err := s.establish(zkconn, zkevents); err != nil {
		return nil, err
	}

	go s.manage()

	return s, nil
}

func (s *ZkSession) establish(zkconn *zookeeper.Conn, zkevents <-chan zookeeper.Event) error {
	defer s.mu.Unlock()
	s.mu.Lock()

	for {
		select {
		case event, ok := <-zkevents:
			if !ok {
				return ErrZkSessionEventChannelClosed
			}

			if event.State == zookeeper.STATE_EXPIRED_SESSION {
				_ = zkconn.Close()
				return ErrZkSessionExpired
			}

			// Only the STATE_CONNECTED session event is valid when establishing a new session.
			// Any other state is ignored.
			if event.State == zookeeper.STATE_CONNECTED {
				s.Connection = zkconn
				s.events = zkevents
				return nil
			}
		case <-time.After(s.dialTimeout):
			_ = zkconn.Close()
			return ErrZkSessionTimeout
		}
	}

	_ = zkconn.Close()
	return ErrZkSessionNotConnected
}

func (s *ZkSession) Close() error {
	defer s.mu.Unlock()
	s.mu.Lock()

	return s.Connection.Close()
}

func (s *ZkSession) manage() {
	for {
		select {
		case event, ok := <-s.events:
			if !ok {
				return
			}

			switch event.State {
			case zookeeper.STATE_EXPIRED_SESSION:
				zkconn, zkevents, err := zookeeper.Redial(s.servers, s.recvTimeout, s.Connection.ClientId())
				if err != nil {
					return
				}
				for {
					if err := s.establish(zkconn, zkevents); err != nil {
						if err == ErrZkSessionExpired {
							continue
						}
						return
					}
					break
				}
			case zookeeper.STATE_CLOSED:
				return
			}
		}
	}
}
