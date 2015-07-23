package session

import (
	"errors"

	"github.com/Shopify/gozk"
)

// CreateAndMaintainEphemeral creates an ephemeral znode with the given
// path+data, and signals the provided channel when it has stopped. This
// indicates the node no longer exists, either because the connection was
// closed or an error occurred. As long as the channel has not been signalled,
// the caller can reasonable expect that the ephemeral node still exists.
//
// This is not an appropriate construct to use for locking, as a partition will
// not be immediately reported to the caller; the code will wait for a
// reconnect or expiry before notifying.
func (z *ZKSession) CreateAndMaintainEphemeral(path, data string, dead chan<- error) error {
	doCreate := func() error {
		_, err := z.conn.Create(path, data, zookeeper.EPHEMERAL, defaultACLs)
		return err
	}

	if err := doCreate(); err != nil {
		return err
	}

	evs := make(chan ZKSessionEvent)
	z.Subscribe(evs)

	go func() { dead <- maintainEphemeral(evs, doCreate) }()
}

func maintainEphemeral(evs <-chan ZKSessionEvent, doCreate func() error) error {
	for ev := range evs {
		switch ev.State {
		case SessionClosed:
			// Someone called Close() on the session; we are presumably expected to
			// shut down gracefully. The node will already be removed by the
			// connection teardown.
			return nil
		case SessionFailed:
			return errors.New("the session was terminated")
		case SessionDisconnected:
			// nothing to do yet. Eventually we hope to receive one of the
			// Reconnected events
		case SessionReconnected:
			// All is fine; we reconnected before our ephemeral node expired
		case SessionExpiredReconnected:
			// We reconnected, but it took a while, and we must recreate our
			// ephemeral node.
			if err := doCreate(); err != nil {
				return err
			}
		}
	}
}
