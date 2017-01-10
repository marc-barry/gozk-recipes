package session

import (
	"testing"
	"time"

	zookeeper "github.com/Shopify/gozk"
	toxiproxy "github.com/Shopify/toxiproxy/client"
)

func CreateProxy(t *testing.T) (*toxiproxy.Proxy){
	client := toxiproxy.NewClient("http://localhost:8474")
	proxy, err := client.CreateProxy("gozk_test_zookeeper", "localhost:27445", "localhost:2181")
	if err != nil {
		t.Fatal("Couldn't create proxy. Is toxiproxy running? Error: ", err)
	}
	return proxy
}

func TestReceiveEventWhenSubscribing(t *testing.T) {
	proxy := CreateProxy(t)
	defer proxy.Delete()

	store, err := NewZKSession("localhost:27445", 200*time.Millisecond, nil)
	if err != nil {
		t.Error("Failed to connect to Zookeeper: ", err)
	}
	defer store.Close()

	events := make(chan ZKSessionEvent)
	store.Subscribe(events)

	go func() {
		err := proxy.Disable()
		if err != nil {
			t.Error("Failed to delete proxy: ", err)
		}

		_, _, err = store.Children("/")
		if err == nil {
			t.Error("Expected error when listing children")
		}

		err = proxy.Enable()
		if err != nil {
			t.Error("Failed to create proxy: ", err)
		}
	}()

	select {
	case event := <-events:
		if event != SessionDisconnected {
			t.Error("Expected to receive disconnected: ", event)
		}
	case <-time.After(5 * time.Second):
		t.Error("Failed to receive event")
	}

	select {
	case event := <-events:
		if event != SessionReconnected {
			t.Error("Expected to receive reconnected: ", event)
		}
	case <-time.After(5 * time.Second):
		t.Error("Failed to receive event")
	}
}

func TestResumeZKSessionWithValidSession(t *testing.T) {
	proxy := CreateProxy(t)
	defer proxy.Delete()

	store, err := NewZKSession("localhost:27445", 200*time.Millisecond, nil)
	if err != nil {
		t.Error("Failed to connect to Zookeeper: ", err)
	}

	events := make(chan ZKSessionEvent)
	store.Subscribe(events)

	existingClientId, err := store.ClientId().Save()
	if err != nil {
		t.Error("Failed to save clientId: ", err)
	}

	clientId, err := zookeeper.LoadClientId(existingClientId)
	if err != nil {
		t.Error("Error loading clientId: ", err)
	}

	//ResumeZKSession is expected to automatically close any previously existing sessions.
	resumeStore, err := ResumeZKSession("localhost:27445", 200*time.Millisecond, nil, clientId)
	if err != nil {
		t.Error("Failed to resume session with Zookeeper: ", err)
	}
	defer resumeStore.Close()

	select {
	case event := <-events:
		if event != SessionDisconnected {
			t.Error("Expected to receive disconnected: ", event)
		}
	case <-time.After(5 * time.Second):
		t.Error("Failed to receive event")
	}
}

func TestResumeZKSessionFailsWithInvalidClientId(t *testing.T) {
	proxy := CreateProxy(t)
	defer proxy.Delete()

	clientId := make([]byte, 24)
	for i := 0; i < 24; i++ {
		clientId[i] = 9
	}

	invalidClientId, err := zookeeper.LoadClientId(clientId)
	if err != nil {
		t.Error("Error loading clientId: ", err)
	}

	_, err = ResumeZKSession("localhost:27445", 200*time.Millisecond, nil, invalidClientId)
	if err == nil {
		t.Error("Resumed session with Zookeeper using incorrect clientId.")
	}
}
