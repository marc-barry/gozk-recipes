package session

import (
	"testing"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/client"
)

func TestReceiveEventWhenSubscribing(t *testing.T) {
	client := toxiproxy.NewClient("http://localhost:8474")
	proxy := client.NewProxy(&toxiproxy.Proxy{Name: "gozk_test_zookeeper", Listen: "localhost:27445", Upstream: "localhost:2181", Enabled: true})

	err := proxy.Create()
	if err != nil {
		t.Fatal("Couldn't create proxy. Is toxiproxy running? Error: ", err)
	}
	defer proxy.Delete()

	store, err := NewZKSession("localhost:27445", 200*time.Millisecond, nil)
	if err != nil {
		t.Error("Failed to connect to Zookeeper: ", err)
	}
	defer store.Close()

	events := make(chan ZKSessionEvent)
	store.Subscribe(events)

	go func() {
		err := proxy.Delete()
		if err != nil {
			t.Error("Failed to delete proxy: ", err)
		}

		_, _, err = store.Children("/")
		if err == nil {
			t.Error("Expected error when listing children")
		}

		err = proxy.Create()
		if err != nil {
			t.Error("Failed to create proxy: ", err)
		}
	}()

	select {
	case event := <-events:
		if event != SessionDisconnected {
			t.Error("Expected to receive disconnected: ", event)
		}

		if <-events != SessionReconnected {
			t.Error("Expected to receive reconnected: ", event)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Failed to receive event")
	}
}
