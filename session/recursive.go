package session

import (
	"sort"
	"strings"

	"github.com/Shopify/gozk"
)

var defaultACLs = zookeeper.WorldACL(zookeeper.PERM_ALL)

// ChildrenRecursive returns a slice all of a node's descendents that are at
// most `maxDepth` levels away from the root.
func (s *ZKSession) ChildrenRecursive(path string, maxDepth int) ([]string, error) {
	rootDepth := 0
	if path != "/" {
		rootDepth = strings.Count(path, "/")
	}

	stat, err := s.Exists(path)
	if err != nil {
		return nil, err
	}

	if stat == nil {
		return []string{}, nil
	}

	nodes := []string{path}
	for index := 0; index < len(nodes); index++ {
		if maxDepth <= 0 || strings.Count(nodes[index], "/") < rootDepth+maxDepth {
			if children, _, err := s.Children(nodes[index]); err == nil {
				parent := nodes[index]
				if parent == "/" {
					parent = ""
				}

				for _, child := range children {
					nodes = append(nodes, parent+"/"+child)
				}
			}
		}
	}
	return nodes[1:], nil
}

// CreateRecursiveAndSet will set data for the given path, creating all parents
// as necessary.
func (s *ZKSession) CreateRecursiveAndSet(path string, data string) error {
	// Since the Set method requires us to create intermediate nodes, we have to
	// do a little extra work here
	index := 0
	for {
		distanceToNextSlash := strings.Index(path[index+1:], "/")
		if distanceToNextSlash < 0 {
			break
		}

		index += distanceToNextSlash + 1
		stat, err := s.Exists(path[:index])
		if err != nil {
			return err
		}

		if stat == nil {
			if _, err := s.Create(path[:index], "", 0, defaultACLs); err != nil {
				return err
			}
		}
	}

	stat, err := s.Set(path, data, -1)
	if stat == nil {
		_, err = s.Create(path, data, 0, defaultACLs)
	}

	return err
}

type nodePaths []string

func (s nodePaths) Len() int           { return len(s) }
func (s nodePaths) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s nodePaths) Less(i, j int) bool { return len(s[i]) < len(s[j]) }

// DeleteRecursive removes a given path and all of its descendents.
func (s *ZKSession) DeleteRecursive(path string) error {
	children, err := s.ChildrenRecursive(path, -1)
	if err != nil {
		return err
	}

	sort.Sort(sort.Reverse(nodePaths(children)))

	for _, child := range children {
		if err := s.Delete(child, -1); err != nil {
			return err
		}
	}
	return s.Delete(path, -1)
}
