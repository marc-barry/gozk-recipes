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
	mu          sync.Mutex

	subscriptions []chan<- ZKSessionEvent
}

func NewZKSession(servers string, recvTimeout time.Duration) (*ZKSession, error) {
	conn, events, err := zookeeper.Dial(servers, recvTimeout)
	if err != nil {
		return nil, err
	}

	s := &ZKSession{
		servers:       servers,
		recvTimeout:   recvTimeout,
		conn:          conn,
		clientID:      conn.ClientId(),
		events:        events,
		subscriptions: make([]chan<- ZKSessionEvent, 0),
	}

	if (<-events).State != zookeeper.STATE_CONNECTED {
		return nil, ErrZKSessionNotConnected
	}

	go s.manage()

	return s, nil
}

func (s *ZKSession) Subscribe(subscription chan<- ZKSessionEvent) {
	s.subscriptions = append(s.subscriptions, subscription)
}

func (s *ZKSession) notifySubscribers(event ZKSessionEvent) {
	for _, subscriber := range s.subscriptions {
		subscriber <- event
	}
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
					s.notifySubscribers(SessionFailed)
					return
				}

			case zookeeper.STATE_AUTH_FAILED:
				s.notifySubscribers(SessionFailed)

			case zookeeper.STATE_CONNECTING:
				s.notifySubscribers(SessionDisconnected)

			case zookeeper.STATE_ASSOCIATING:
				// No action to take, this is fine.

			case zookeeper.STATE_CONNECTED:
				if expired {
					s.notifySubscribers(SessionExpiredReconnected)
					expired = false
				} else {
					s.notifySubscribers(SessionReconnected)
				}
			case zookeeper.STATE_CLOSED:
				s.notifySubscribers(SessionClosed)
			}
		}
	}
}

func (s *ZKSession) ACL(path string) ([]zookeeper.ACL, *zookeeper.Stat, error) {
	return s.conn.ACL(path)
}

func (s *ZKSession) AddAuth(scheme, cert string) error {
	return s.conn.AddAuth(scheme, cert)
}

func (s *ZKSession) Children(path string) ([]string, *zookeeper.Stat, error) {
	return s.conn.Children(path)
}

func (s *ZKSession) ChildrenW(path string) ([]string, *zookeeper.Stat, <-chan zookeeper.Event, error) {
	return s.conn.ChildrenW(path)
}

func (s *ZKSession) ClientId() *zookeeper.ClientId {
	return s.conn.ClientId()
}

func (s *ZKSession) Close() error {
	return s.conn.Close()
}

func (s *ZKSession) Create(path string, value string, flags int, aclv []zookeeper.ACL) (string, error) {
	return s.conn.Create(path, value, flags, aclv)
}

func (s *ZKSession) Delete(path string, version int) error {
	return s.conn.Delete(path, version)
}

func (s *ZKSession) Exists(path string) (*zookeeper.Stat, error) {
	return s.conn.Exists(path)
}

func (s *ZKSession) ExistsW(path string) (*zookeeper.Stat, <-chan zookeeper.Event, error) {
	return s.conn.ExistsW(path)
}

func (s *ZKSession) Get(path string) (string, *zookeeper.Stat, error) {
	return s.conn.Get(path)
}

func (s *ZKSession) GetW(path string) (string, *zookeeper.Stat, <-chan zookeeper.Event, error) {
	return s.conn.GetW(path)
}

func (s *ZKSession) Set(path string, value string, version int) (*zookeeper.Stat, error) {
	return s.conn.Set(path, value, version)
}

func (s *ZKSession) RetryChange(path string, flags int, acl []zookeeper.ACL, changeFunc zookeeper.ChangeFunc) error {
	return s.conn.RetryChange(path, flags, acl, changeFunc)
}

func (s *ZKSession) SetACL(path string, aclv []zookeeper.ACL, version int) error {
	return s.conn.SetACL(path, aclv, version)
}
