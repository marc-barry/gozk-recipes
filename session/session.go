package session

import (
	"sync"
	"time"

	"launchpad.net/gozk"
)

type ZKSessionEvent uint

const (
	SessionConnected ZKSessionEvent = iota
	SessionClosed
	SessionDisconnected
	SessionReconnected
	SessionFailed
)

type ZKSession struct {
	servers     string
	recvTimeout time.Duration
	conn        *zookeeper.Conn
	clientId    *zookeeper.ClientId
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
		clientId:    conn.ClientId(),
		events:      events,
		sEvents:     make(chan ZKSessionEvent),
	}

	go s.manage()

	return s, s.sEvents, nil
}

func (s *ZKSession) manage() {
	prevConnected := false
	for {
		select {
		case event := <-s.events:
			switch event.State {
			case zookeeper.STATE_EXPIRED_SESSION:
				conn, events, err := zookeeper.Redial(s.servers, s.recvTimeout, s.clientId)
				if err == nil {
					s.mu.Lock()
					s.conn = conn
					s.events = events
					s.clientId = conn.ClientId()
					s.mu.Unlock()
				}
				if err != nil {
					s.sEvents <- SessionFailed
					return
				}

			case zookeeper.STATE_AUTH_FAILED:
				s.sEvents <- SessionFailed

			case zookeeper.STATE_CONNECTING:
				if prevConnected {
					s.sEvents <- SessionDisconnected
				}

			case zookeeper.STATE_ASSOCIATING:
				// No action to take, this is fine.

			case zookeeper.STATE_CONNECTED:
				if prevConnected {
					s.sEvents <- SessionReconnected
				}
				if !prevConnected {
					s.sEvents <- SessionConnected
					prevConnected = true
				}

			case zookeeper.STATE_CLOSED:
				s.sEvents <- SessionClosed
			}
		}
	}
}

func (s *ZKSession) Children(path string) (children []string, stat *zookeeper.Stat, err error) {
	defer s.mu.Unlock()
	s.mu.Lock()

	return s.conn.Children(path)
}

func (s *ZKSession) Create(path, value string, flags int, aclv []zookeeper.ACL) (pathCreated string, err error) {
	defer s.mu.Unlock()
	s.mu.Lock()

	return s.conn.Create(path, value, flags, aclv)
}

func (s *ZKSession) Delete(path string, version int) (err error) {
	defer s.mu.Unlock()
	s.mu.Lock()

	return s.conn.Delete(path, version)
}

func (s *ZKSession) Exists(path string) (stat *zookeeper.Stat, err error) {
	defer s.mu.Unlock()
	s.mu.Lock()

	return s.conn.Exists(path)
}

func (s *ZKSession) ExistsW(path string) (stat *zookeeper.Stat, watch <-chan zookeeper.Event, err error) {
	defer s.mu.Unlock()
	s.mu.Lock()

	return s.conn.ExistsW(path)
}

func (s *ZKSession) Close() error {
	defer s.mu.Unlock()
	s.mu.Lock()

	return s.conn.Close()
}
