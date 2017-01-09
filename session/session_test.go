package session

import (
	"testing"
	"time"
	toxiproxy "github.com/Shopify/toxiproxy/client"
)

func TestReceiveEventWhenSubscribing(t *testing.T) {
	client := toxiproxy.NewClient("http://localhost:8474")
	proxy, err := client.CreateProxy("gozk_test_zookeeper", "localhost:27445", "localhost:2181")
	if err != nil {
		t.Fatal("Couldn't create proxy. Is toxiproxy running? Error: ", err)
	}
	defer proxy.Disable()

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

