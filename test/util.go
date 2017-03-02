package test

import (
	"os"
	"testing"

	toxiproxy "github.com/Shopify/toxiproxy/client"
)

const PROXY_PORT = "27445"

func CreateProxy(t *testing.T) *toxiproxy.Proxy {
	url := GetToxiProxyURL(t)
	zks := GetZooKeepers(t)
	host := GetToxiProxyHost(t)

	client := toxiproxy.NewClient(url)
	proxy, err := client.CreateProxy("gozk_test_zookeeper", host+":"+PROXY_PORT, zks)
	if err != nil {
		t.Fatal("Couldn't create proxy. Is toxiproxy running? Error: ", err)
	}
	return proxy
}

func GetToxiProxyURL(t *testing.T) string {
	if os.Getenv("TOXIPROXY_URL") == "" {
		t.Fatal("TOXIPROXY_URL environment variable must be defined")
	}
	return os.Getenv("TOXIPROXY_URL")
}

func GetToxiProxyHost(t *testing.T) string {
	if os.Getenv("TOXIPROXY_HOST") == "" {
		t.Fatal("TOXIPROXY_HOST environment variable must be defined")
	}
	return os.Getenv("TOXIPROXY_HOST")
}

func GetZooKeepers(t *testing.T) string {
	if os.Getenv("ZOOKEEPERS") == "" {
		t.Fatal("ZOOKEEPERS environment variable must be defined")
	}
	return os.Getenv("ZOOKEEPERS")
}
