package ephemeral

import (
	"testing"
	"time"

	"github.com/Shopify/gozk-recipes/session"
	toxiproxy "github.com/Shopify/toxiproxy/client"
)

func TestCreateAndMaintain(t *testing.T) {
	client := toxiproxy.NewClient("http://localhost:8474")
	proxy := client.NewProxy(&toxiproxy.Proxy{Name: "gozk_test_zookeeper", Listen: "localhost:27445", Upstream: "localhost:2181", Enabled: true})

	err := proxy.Create()
	if err != nil {
		t.Fatal("Couldn't create proxy. Is toxiproxy running? Error: ", err)
	}
	defer proxy.Delete()

	store, err := session.NewZKSession("localhost:27445", 200*time.Millisecond, nil)
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

		if err := proxy.Delete(); err != nil {
			t.Error("Failed to delete proxy: ", err)
		}

		assertZnodePresence(t, store, "/eph", false)

		if err = proxy.Create(); err != nil {
			t.Error("Failed to create proxy: ", err)
		}

		assertZnodePresence(t, store, "/eph", true)

		if err := proxy.Delete(); err != nil {
			t.Error("Failed to delete proxy: ", err)
		}

		println("waiting 10.5 seconds for zookeeper to purge an ephemeral node...")
		time.Sleep(10500 * time.Millisecond)
		println("re-establishing proxy")

		if err = proxy.Create(); err != nil {
			t.Error("Failed to create proxy: ", err)
		}

		// This is technically racy, but it's pretty safe to assume we'll complete before the writer, I think.
		assertZnodePresence(t, store, "/eph", false)

		// Give it a little bit of time to recreate the node.
		time.Sleep(500 * time.Millisecond)

		// TODO(burke): assert that this znode's ctime is less than a second ago.
		assertZnodePresence(t, store, "/eph", true)

		if err := proxy.Delete(); err != nil {
			t.Error("Failed to delete proxy: ", err)
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
