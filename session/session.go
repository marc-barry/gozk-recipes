package session

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	zookeeper "github.com/Shopify/gozk"
)

type ZKSessionEvent uint

// NewZKSession is passed a logger to log ZK events.
type stdLogger interface {
	Printf(format string, v ...interface{})
}

// nullLogger is used when a nil interface is given
type nullLogger struct{}

func (l *nullLogger) Printf(format string, v ...interface{}) {}

// ErrZKSessionNotConnected is analogous to the SessionFailed event, but returned as an error from NewZKSession on initialization.
var ErrZKSessionNotConnected = errors.New("unable to connect to ZooKeeper")

// ErrZKSessionDisconnected indicates the session *was* connected, but has
// become disconnected in a way deemed unrecoverable.
var ErrZKSessionDisconnected = errors.New("connection to ZooKeeper was lost")

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

	DefaultRecvTimeout = 5 * time.Second
)

type SessionOpts struct {
	recvTimeout time.Duration
	logger      stdLogger
	clientID    *zookeeper.ClientId
	servers     []string
	dnsRefresh  time.Duration
}

// Create initializes a new session with the settings in s.
func (s SessionOpts) Create() (*ZKSession, error) {
	var conn *zookeeper.Conn
	var events <-chan zookeeper.Event
	var err error

	if len(s.servers) == 0 {
		return nil, fmt.Errorf("no zookeeper servers specified")
	}

	servers := strings.Join(s.servers, ",")
	if s.clientID == nil {
		conn, events, err = zookeeper.Dial(servers, s.recvTimeout)
	} else {
		conn, events, err = zookeeper.Redial(servers, s.recvTimeout, s.clientID)
	}

	if err != nil {
		return nil, err
	}

	conn.SetServersResolutionDelay(s.dnsRefresh)

	session := &ZKSession{
		servers:       servers,
		recvTimeout:   s.recvTimeout,
		conn:          conn,
		clientID:      conn.ClientId(),
		events:        events,
		subscriptions: make([]chan<- ZKSessionEvent, 0),
		log:           s.logger,
	}

	err = waitForConnection(events)
	if err != nil {
		_ = session.conn.Close()
		return nil, fmt.Errorf("waiting for initial connection: %w", err)
	}
	server, _ := session.CurrentServer()
	s.logger.Printf("session established with %s", server)

	return session, nil
}

func waitForConnection(events <-chan zookeeper.Event) error {
	for {
		select {
		case event := <-events:
			switch event.State {
			case zookeeper.STATE_AUTH_FAILED, zookeeper.STATE_EXPIRED_SESSION, zookeeper.STATE_CLOSED:
				return ErrZKSessionNotConnected
			case zookeeper.STATE_CONNECTED:
				return nil
			}
		case <-time.After(5 * time.Second):
			return ErrZKSessionNotConnected
		}
	}
}

type SessionOpt func(SessionOpts) SessionOpts

// WithRecvTimeout creates a session with the given timeout.
func WithRecvTimeout(timeout time.Duration) SessionOpt {
	return func(so SessionOpts) SessionOpts {
		so.recvTimeout = timeout
		return so
	}
}

// WithLogger creates a session with the given logger.
func WithLogger(logger stdLogger) SessionOpt {
	return func(so SessionOpts) SessionOpts {
		// Maintain backwards compatibility
		if logger == nil {
			logger = &nullLogger{}
		}
		so.logger = logger
		return so
	}
}

// WithZookeepers creates a session with the given zookeeper hosts.
func WithZookeepers(zookeepers []string) SessionOpt {
	return func(so SessionOpts) SessionOpts {
		so.servers = zookeepers
		return so
	}
}

// WithZookeeperClientID creates a session with the given client ID.
func WithZookeeperClientID(id *zookeeper.ClientId) SessionOpt {
	return func(so SessionOpts) SessionOpts {
		so.clientID = id
		return so
	}
}

// WithZookeeperClientID creates a session with periodic DNS refresh enabled.
func WithDNSRefresh(duration time.Duration) SessionOpt {
	return func(so SessionOpts) SessionOpts {
		so.dnsRefresh = duration
		return so
	}
}

type ZKSession struct {
	servers     string
	recvTimeout time.Duration
	conn        *zookeeper.Conn
	clientID    *zookeeper.ClientId
	events      <-chan zookeeper.Event
	mu          sync.Mutex

	subscriptions []chan<- ZKSessionEvent
	log           stdLogger
}

func ResumeZKSession(servers string, recvTimeout time.Duration, logger stdLogger, clientId *zookeeper.ClientId) (*ZKSession, error) {
	return NewSessionWithOpts(
		WithLogger(logger),
		WithZookeepers(strings.Split(servers, ",")),
		WithRecvTimeout(recvTimeout),
		WithZookeeperClientID(clientId),
	)
}

func NewSessionWithOpts(opts ...SessionOpt) (*ZKSession, error) {
	sessionOpts := SessionOpts{
		logger:      &nullLogger{},
		recvTimeout: DefaultRecvTimeout,
	}

	for _, so := range opts {
		sessionOpts = so(sessionOpts)
	}

	session, err := sessionOpts.Create()
	if err != nil {
		return nil, fmt.Errorf("creating zookeeper session: %w", err)
	}

	go session.manage()

	return session, nil
}

func NewZKSession(servers string, recvTimeout time.Duration, logger stdLogger) (*ZKSession, error) {
	return NewSessionWithOpts(
		WithLogger(logger),
		WithZookeepers(strings.Split(servers, ",")),
		WithRecvTimeout(recvTimeout),
	)
}

func (s *ZKSession) CurrentServer() (string, error) {
	return s.conn.CurrentServer()
}

func (s *ZKSession) SetServersResolutionDelay(delay time.Duration) {
	s.conn.SetServersResolutionDelay(delay)
}

func (s *ZKSession) Subscribe(subscription chan<- ZKSessionEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subscriptions = append(s.subscriptions, subscription)
}

func (s *ZKSession) notifySubscribers(event ZKSessionEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
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
				s.log.Printf("gozk-recipes/session: got STATE_EXPIRED_SESSION for conn %+v", s.conn)
				expired = true
				conn, events, err := zookeeper.Redial(s.servers, s.recvTimeout, s.clientID)
				if err == nil {
					s.log.Printf("gozk-recipes/session: STATE_EXPIRED_SESSION redialed conn %+v", conn)
					s.mu.Lock()
					if s.conn != nil {
						err := s.conn.Close()
						if err != nil {
							s.log.Printf("gozk-recipes/session: error in closing existing zookeeper connection: %v", err)
						}
					}
					s.conn = conn
					s.events = events
					s.clientID = conn.ClientId()
					s.mu.Unlock()
				}
				if err != nil {
					s.notifySubscribers(SessionFailed)
					s.log.Printf("gozk-recipes/session.SessionFailed: %s, session terminated", err.Error())
					return
				}

			case zookeeper.STATE_AUTH_FAILED:
				s.notifySubscribers(SessionFailed)
				s.log.Printf("gozk-recipes/session.SessionFailed: zookeeper.STATE_AUTH_FAILURE, session terminated")
				return

			case zookeeper.STATE_CONNECTING:
				s.notifySubscribers(SessionDisconnected)
				s.log.Printf("gozk-recipes/session.SessionDisconnected: attempting to reconnect")

			case zookeeper.STATE_ASSOCIATING:
				// No action to take, this is fine.

			case zookeeper.STATE_CONNECTED:
				if expired {
					s.notifySubscribers(SessionExpiredReconnected)
					s.log.Printf("gozk-recipes/session.SessionExpiredReconnected: all ephemeral nodes purged")
					expired = false
				} else {
					s.notifySubscribers(SessionReconnected)
					s.log.Printf("gozk-recipes/session.SessionReconnected: reconnected before timed out")
				}
			case zookeeper.STATE_CLOSED:
				s.notifySubscribers(SessionClosed)
				s.log.Printf("gozk-recipes/session.SessionClosed: normally caused by call to Close(), session terminated")
				return
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
