package ephemeral

import (
	"testing"
	"time"

	"github.com/Shopify/gozk-recipes/session"
	"github.com/Shopify/gozk-recipes/test"
)

func TestCreateAndMaintain(t *testing.T) {
	proxy := test.CreateProxy(t)
	defer proxy.Delete()

	err := proxy.Enable()
	if err != nil {
		t.Fatal("Couldn't enable proxy. Is toxiproxy running? Error: ", err)
	}

	store, err := session.NewZKSession(test.GetToxiProxyHost(t)+":"+test.PROXY_PORT, 200*time.Millisecond, nil)
	if err != nil {
		t.Error("Failed to connect to Zookeeper: ", err)
	}
	defer store.Close()

	dead := make(chan error)
	if err := CreateAndMaintain(store, "/eph", "whatever", dead); err != nil {
		t.Error("CreateAndMaintainEphemeral failed: ", err)
	}

	events := make(chan session.ZKSessionEvent)
	store.Subscribe(events)

	go func() {
		err := proxy.Disable()
		if err != nil {
			t.Error("Failed to disable proxy: ", err)
		}

		assertZnodePresence(t, store, "/eph", false)

		err = proxy.Enable()
		if err != nil {
			t.Error("Failed to enable proxy: ", err)
		}

		assertZnodePresence(t, store, "/eph", true)

		err = proxy.Disable()
		if err != nil {
			t.Error("Failed to disable proxy: ", err)
		}

		println("waiting 10.5 seconds for zookeeper to purge an ephemeral node...")
		time.Sleep(10500 * time.Millisecond)
		println("re-establishing proxy")

		err = proxy.Enable()
		if err != nil {
			t.Error("Failed to enable proxy: ", err)
		}

		// This is technically racy, but it's pretty safe to assume we'll complete before the writer, I think.
		assertZnodePresence(t, store, "/eph", false)

		// Give it a little bit of time to recreate the node.
		time.Sleep(500 * time.Millisecond)

		// TODO(burke): assert that this znode's ctime is less than a second ago.
		assertZnodePresence(t, store, "/eph", true)

		err = proxy.Disable()
		if err != nil {
			t.Error("Failed to disable proxy: ", err)
		}
	}()

	assertEvent(t, events, session.SessionDisconnected)
	assertEvent(t, events, session.SessionReconnected)
	assertEvent(t, events, session.SessionDisconnected)
	assertEvent(t, events, session.SessionExpiredReconnected)
	assertEvent(t, events, session.SessionDisconnected)

	println("waiting 20 seconds for maintainEphemeral to time out waiting for connection re-establishment")
	start := time.Now()

	select {
	case err := <-dead:
		if err != session.ErrZKSessionDisconnected {
			t.Error("Expected ErrZKSessionDisconnected, but got:", err)
		}
		elapsed := time.Since(start)
		if elapsed > 21*time.Second || elapsed < 19*time.Second {
			t.Error("session disconnect took an unexpected amount of time...", elapsed)
		}
	case <-time.After(30 * time.Second):
		t.Error("Expected ephemeral to indicate an error, but it didn't detect the disconnect")
	}
}

func assertEvent(t *testing.T, events chan session.ZKSessionEvent, exp session.ZKSessionEvent) {
	select {
	case act := <-events:
		if act != exp {
			t.Errorf("Expected to receive event %d but got %d", exp, act)
		}
	case <-time.After(16000 * time.Millisecond):
		t.Error("Failed to receive event")
	}
}

func assertZnodePresence(t *testing.T, store *session.ZKSession, path string, presence bool) {
	_, _, err := store.Get(path)

	if presence {
		if err != nil {
			t.Error("expected znode to exist, but it did not")
		}
	} else {
		if err == nil {
			t.Error("expected znode to not exist, but it did")
		}
	}
}
