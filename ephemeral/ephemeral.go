package ephemeral

import (
	"time"

	"github.com/Shopify/gozk"
	"github.com/Shopify/gozk-recipes/session"
)

// how long to wait after a disconnect event before considering the current
// state unrecoverable.
var maxWait = 20 * time.Second

// CreateAndMaintain creates an ephemeral znode with the given path+data, and
// signals the provided channel when it has stopped. This indicates the node no
// longer exists, either because the connection was closed or an error
// occurred. As long as the channel has not been signalled, the caller can
// reasonable expect that the ephemeral node still exists.
//
// This is not an appropriate construct to use for locking, as a partition will
// not be immediately reported to the caller; the code will wait for a
// reconnect or expiry before notifying.
func CreateAndMaintain(z *session.ZKSession, path, data string, dead chan<- error) error {
	doCreate := func() error {
		_, err := z.Create(path, data, zookeeper.EPHEMERAL, zookeeper.WorldACL(zookeeper.PERM_ALL))
		return err
	}

	if err := doCreate(); err != nil {
		return err
	}

	evs := make(chan session.ZKSessionEvent)
	z.Subscribe(evs)

	go func() { dead <- maintainEphemeral(evs, doCreate) }()
	return nil
}

func maintainEphemeral(evs <-chan session.ZKSessionEvent, doCreate func() error) error {
	broken := make(chan struct{})
	reconnected := make(chan struct{}, 1)
	for {
		select {
		case <-broken:
			return session.ErrZKSessionDisconnected
		case ev := <-evs:
			switch ev {
			case session.SessionClosed:
				// Someone called Close() on the session; we are presumably expected to
				// shut down gracefully. The node will already be removed by the
				// connection teardown.
				return nil
			case session.SessionFailed:
				return session.ErrZKSessionDisconnected
			case session.SessionDisconnected:
				// If the connection isn't re-established before 20 seconds have
				// elapsed, freak out.
				go func() {
					select {
					case <-reconnected:
					case <-time.After(maxWait):
						broken <- struct{}{}
					}
				}()
			case session.SessionReconnected:
				// All is fine; we reconnected before our ephemeral node expired.
				// Stop the disconnect countdown.
				select {
				case reconnected <- struct{}{}:
				default:
				}
			case session.SessionExpiredReconnected:
				// We reconnected, but it took a while, and we must recreate our
				// ephemeral node.
				// Stop the disconnect countdown first.
				select {
				case reconnected <- struct{}{}:
				default:
				}
				if err := doCreate(); err != nil {
					return err
				}
			}
		}
	}
}
