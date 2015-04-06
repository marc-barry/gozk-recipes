package lock

/**
See the lock recipe in the ZooKeeper documentation for more details.

The following are the basics for using ZooKeeper to implement a global synchronous lock.
(1) Call Create() with a pathname "{root}/_locknode" and the zookeeper.EPHEMERAL and zookeeper.SEQUENCE flags set.
(2) Call Children() on the lock node. Note this is not a watch to avoid the herd effect.
(3) If the pathname created in step 1 has the lowest sequence number, the client has the lock and the client has the lock.
(4) Else, ihe client calls Exists() with the watch flag set on the path in the lock directory with the next lowest sequence number.
(5) If Exists() returns false, go to step 2.
(6) Otherwise, wait for a notification for the pathname from the previous step before going to step 2.

Clients wishing to release a lock simply delete the node they created in step 1.

Here are a few things of note:

- The removal of a node will only cause one client to wake up since each node is watched by exactly one client. In this way, you avoid the herd effect.
**/

import (
	"fmt"
	"launchpad.net/gozk"
	"path"
	"sort"

	"github.com/shopify/gozk-recipes/session"
)

type GlobalLock struct {
	Session       *session.ZKSession
	root          string
	ephemeralPath string
	data          string
}

func NewGlobalLock(session *session.ZKSession, root string, data string) (*GlobalLock, error) {
	if stat, _ := session.Exists(root); stat == nil {
		_, err := session.Create(root, "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
		if err != nil {
			if stat, _ := session.Exists(root); stat == nil {
				return nil, err
			}
		}
	}
	return &GlobalLock{session, root, "", data}, nil
}

func (g *GlobalLock) Destroy() error {
	children, _, err := g.Session.Children(g.root)
	if err != nil {
		return err
	}

	if len(children) == 0 {
		return g.Session.Delete(g.root, -1)
	}

	return nil
}

func (g *GlobalLock) Lock() (err error) {
	if len(g.ephemeralPath) > 0 {
		if stat, _ := g.Session.Exists(g.ephemeralPath); stat != nil {
			return nil
		}
	}

	// (1)
	g.ephemeralPath, err = g.Session.Create(g.root+"/", g.data, zookeeper.EPHEMERAL|zookeeper.SEQUENCE, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil {
		return err
	}

	var children []string

	for {
		// (2)
		children, _, err = g.Session.Children(g.root)

		// The children nodes with be the sequence values --> 1, 2, 3....
		sort.Strings(children)

		if len(children) == 0 {
			return fmt.Errorf("Lock in unknown state. Ephemeral path %s exists but there are no children.", g.ephemeralPath)
		}

		// (3)
		if children[0] == path.Base(g.ephemeralPath) {
			return nil
		}

		myIndex := sort.SearchStrings(children, path.Base(g.ephemeralPath))

		for {
			// (4)
			stat, w, err := g.Session.ExistsW(g.root + "/" + children[myIndex-1])
			if err != nil {
				return err
			}
			// (5)
			if stat == nil {
				break
			}
			// (6)
			<-w
		}
	}

	return nil
}

func (g *GlobalLock) Unlock() error {
	var err error = nil
	if len(g.ephemeralPath) > 0 {
		err := g.Session.Delete(g.ephemeralPath, -1)
		if err == nil {
			g.ephemeralPath = ""
		}
	}
	return err
}
