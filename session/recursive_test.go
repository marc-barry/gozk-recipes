package session

import (
	"testing"
	"time"

	zookeeper "github.com/Shopify/gozk"
	"github.com/Shopify/gozk-recipes/test"
	"github.com/stretchr/testify/assert"
)

func AssertNodeValueEqual(t *testing.T, session *ZKSession, path, expected string) {
	data, _, err := session.Get(path)
	if err != nil {
		t.Error("Get error: ", err)
	}
	assert.Equal(t, expected, data)
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
	store, err := NewZKSession(test.GetZooKeepers(t), 200*time.Millisecond, nil)
	if err != nil {
		t.Error("Failed to connect to Zookeeper: ", err)
	}
	defer store.Close()

	// Scrappy way of insuring that the /test node (which may have already existed) has been deleted with each test
	// run
	store.DeleteRecursive("/test")

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

		assert.ElementsMatch(t, []string{}, children)
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

		assert.ElementsMatch(t, nodes[1:], children)
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

		assert.ElementsMatch(t, []string{"/test/foo", "/test/bar", "/test/bar/eggs"}, children)
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
