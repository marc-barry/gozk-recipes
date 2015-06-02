package session

import (
	"io/ioutil"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/Shopify/gozk"
)

func AssertEqual(t *testing.T, expected, actual interface{}) {
	switch expected.(type) {
	case []string:
		sort.Strings(expected.([]string))
		sort.Strings(actual.([]string))
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %s, actual %s", expected, actual)
	}
}

func AssertNodeValueEqual(t *testing.T, session *ZKSession, path, expected string) {
	data, _, err := session.Get(path)
	if err != nil {
		t.Error("Get error: ", err)
	}
	AssertEqual(t, expected, data)
}

func AssertNodeDoesNotExist(t *testing.T, session *ZKSession, path string) {
	stat, err := session.Exists(path)
	if err != nil {
		t.Error("Exists error: ", err)
	}

	if stat != nil {
		t.Error("Expected node to not exist: ", path)
	}
}

func AssertNodeExists(t *testing.T, session *ZKSession, path string) {
	stat, err := session.Exists(path)
	if err != nil {
		t.Error("Exists error: ", err)
	}

	if stat == nil {
		t.Error("Expected node to exist: ", path)
	}
}

func withTestStore(t *testing.T, f func(*ZKSession)) {
	runDir, err := ioutil.TempDir("", "zk")
	if err != nil {
		t.Error("Failed to create zookeeper run dir: ", err)
	}
	defer os.RemoveAll(runDir)

	server, err := zookeeper.CreateServer(22447, runDir, "/usr/lib/zookeeper")
	if err != nil {
		t.Error("Failed to create zookeeper server: ", err)
	}
	defer server.Stop()

	if err := server.Start(); err != nil {
		t.Error("Failed to start zookeeper server: ", err)
	}

	store, err := NewZKSession("localhost:22447", 200*time.Millisecond, nil)
	if err != nil {
		t.Error("Failed to connect to Zookeeper: ", err)
	}
	defer store.Close()

	f(store)
}

func initializeZK(t *testing.T, s *ZKSession, nodes ...string) {
	defaultAcls := []zookeeper.ACL{
		zookeeper.ACL{
			Perms:  zookeeper.PERM_ALL,
			Scheme: "world",
			Id:     "anyone",
		},
	}

	for _, node := range nodes {
		if _, err := s.Create(node, "", 0, defaultAcls); err != nil {
			t.Error("Unable to create initial value in zk: ", err)
		}
	}
}

func TestChildrenRecursiveWithNonExistingNodeShouldHaveEmptyResult(t *testing.T) {
	withTestStore(t, func(session *ZKSession) {
		children, err := session.ChildrenRecursive("/test", -1)
		if err != nil {
			t.Error("ChildrenRecursive error: ", err)
		}

		AssertEqual(t, []string{}, children)
	})
}

func TestChildrenRecursiveWithNodesShouldHaveResult(t *testing.T) {
	withTestStore(t, func(session *ZKSession) {
		nodes := []string{"/test", "/test/foo", "/test/bar", "/test/bar/eggs"}
		initializeZK(t, session, nodes...)

		children, err := session.ChildrenRecursive("/test", -1)
		if err != nil {
			t.Error("ChildrenRecursive error: ", err)
		}

		AssertEqual(t, nodes[1:], children)
	})
}

func TestChildrenRecursiveWithLimitedDepthShouldReturnSubset(t *testing.T) {
	withTestStore(t, func(session *ZKSession) {
		nodes := []string{"/test", "/test/foo", "/test/bar", "/test/bar/eggs", "/test/bar/eggs/spam"}
		initializeZK(t, session, nodes...)

		children, err := session.ChildrenRecursive("/test", 2)
		if err != nil {
			t.Error("ChildrenRecursive error: ", err)
		}

		AssertEqual(t, []string{"/test/foo", "/test/bar", "/test/bar/eggs"}, children)
	})
}

func TestCreateRecursiveAndSetWithNoParentsShouldCreateNodes(t *testing.T) {
	withTestStore(t, func(session *ZKSession) {
		if err := session.CreateRecursiveAndSet("/test/foo/bar", "foobar"); err != nil {
			t.Error("CreateRecursiveAndSet error: ", err)
		}

		AssertNodeValueEqual(t, session, "/test/foo/bar", "foobar")
	})
}

func TestCreateRecursiveAndSetWithParentsShouldNotChangeData(t *testing.T) {
	withTestStore(t, func(session *ZKSession) {
		initializeZK(t, session, "/test", "/test/foo")

		if err := session.CreateRecursiveAndSet("/test/foo/bar", "foobar"); err != nil {
			t.Error("CreateRecursiveAndSet error: ", err)
		}

		if _, err := session.Set("/test/foo", "spam", 0); err != nil {
			t.Error("Set error: ", err)
		}

		AssertNodeValueEqual(t, session, "/test/foo", "spam")
		AssertNodeValueEqual(t, session, "/test/foo/bar", "foobar")
	})
}

func TestDeleteRecursiveShouldDelete(t *testing.T) {
	withTestStore(t, func(session *ZKSession) {
		initializeZK(t, session, "/test", "/test/foo", "/test/foo/bar", "/test/foo/bar/spam")

		if err := session.DeleteRecursive("/test/foo"); err != nil {
			t.Error("DeleteRecursive error: ", err)
		}

		AssertNodeDoesNotExist(t, session, "/test/foo/bar/spam")
		AssertNodeDoesNotExist(t, session, "/test/foo/bar")
		AssertNodeDoesNotExist(t, session, "/test/foo")
		AssertNodeExists(t, session, "/test")
	})
}
