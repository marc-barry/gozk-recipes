package session

import (
	"errors"
	"sync"
	"time"

	"launchpad.net/gozk"
)

type ZKSessionEvent uint

// ErrZKSessionNotConnected is analogous to the SessionFailed event, but returned as an error from NewZKSession on initialization.
var ErrZKSessionNotConnected = errors.New("unable to connect to ZooKeeper")

const (
	// SessionClosed is normally only returned as a direct result of calling Close() on the ZKSession object. It is a
	// terminal state; the connection will not be re-established.
	SessionClosed ZKSessionEvent = iota
	// SessionDisconnected is a transient state indicating that the connection to ZooKeeper was lost. The library is
	// attempting to reconnect and you will receive another event when it has. In the meantime, if you're using ZooKeeper
	// to implement, for example, a lock, assume you have lost the lock.
	SessionDisconnected
	// SessionReconnected is returned after a SessionDisconnected event, to indicate that the library was able to re-establish
	// its connection to the zookeeper cluster before the session timed out. Ephemeral nodes have not been torn down, so
	// any created by the previous connection still exist.
	SessionReconnected
	// SessionExpiredReconnected indicates that the session was reconnected (also happens strictly after a SessionDisconnected
	// event), but that the reconnection took longer than the session timeout, and all ephemeral nodes were purged.
	SessionExpiredReconnected
	// SessionFailed indicates that the session failed unrecoverably. This may mean incorrect credentials, or broken quorum,
	// or a partition from the entire ZooKeeper cluster, or any other mode of absolute failure.
	SessionFailed
)

type ZKSession struct {
	servers     string
	recvTimeout time.Duration
	conn        *zookeeper.Conn
	clientID    *zookeeper.ClientId
	events      <-chan zookeeper.Event
	sEvents     chan ZKSessionEvent
	mu          sync.Mutex
}

func NewZKSession(servers string, recvTimeout time.Duration) (*ZKSession, <-chan ZKSessionEvent, error) {
	conn, events, err := zookeeper.Dial(servers, recvTimeout)
	if err != nil {
		return nil, nil, err
	}

	s := &ZKSession{
		servers:     servers,
		recvTimeout: recvTimeout,
		conn:        conn,
		clientID:    conn.ClientId(),
		events:      events,
		sEvents:     make(chan ZKSessionEvent),
	}

	if (<-events).State != zookeeper.STATE_CONNECTED {
		return nil, nil, ErrZKSessionNotConnected
	}

	go s.manage()

	return s, s.sEvents, nil
}

func (s *ZKSession) manage() {
	expired := false
	for {
		select {
		case event := <-s.events:
			switch event.State {
			case zookeeper.STATE_EXPIRED_SESSION:
				expired = true
				conn, events, err := zookeeper.Redial(s.servers, s.recvTimeout, s.clientID)
				if err == nil {
					s.mu.Lock()
					s.conn = conn
					s.events = events
					s.clientID = conn.ClientId()
					s.mu.Unlock()
				}
				if err != nil {
					s.sEvents <- SessionFailed
					return
				}

			case zookeeper.STATE_AUTH_FAILED:
				s.sEvents <- SessionFailed

			case zookeeper.STATE_CONNECTING:
				s.sEvents <- SessionDisconnected

			case zookeeper.STATE_ASSOCIATING:
				// No action to take, this is fine.

			case zookeeper.STATE_CONNECTED:
				if expired {
					s.sEvents <- SessionExpiredReconnected
					expired = false
				} else {
					s.sEvents <- SessionReconnected
				}

			case zookeeper.STATE_CLOSED:
				s.sEvents <- SessionClosed
			}
		}
	}
}

func (s *ZKSession) Children(path string) (children []string, stat *zookeeper.Stat, err error) {
	return s.conn.Children(path)
}

func (s *ZKSession) Create(path, value string, flags int, aclv []zookeeper.ACL) (pathCreated string, err error) {
	return s.conn.Create(path, value, flags, aclv)
}

func (s *ZKSession) Delete(path string, version int) (err error) {
	return s.conn.Delete(path, version)
}

func (s *ZKSession) Exists(path string) (stat *zookeeper.Stat, err error) {
	return s.conn.Exists(path)
}

func (s *ZKSession) ExistsW(path string) (stat *zookeeper.Stat, watch <-chan zookeeper.Event, err error) {
	return s.conn.ExistsW(path)
}

func (s *ZKSession) Close() error {
	return s.conn.Close()
}
