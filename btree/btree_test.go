package btree

import (
	"strings"
	"testing"
	"unsafe"
)

type C struct {
	tree  *BTree
	ref   map[string]string
	pages map[uint64]BNode
}

func newC() *C {
	pages := make(map[uint64]BNode)
	return &C{
		tree: &BTree{
			get: func(page uint64) BNode {
				n, ok := pages[page]
				assert(ok, "page not found")
				return n
			},
			new: func(n BNode) uint64 {
				assert(n.nbytes() <= BTREE_PAGE_SIZE, "node too big")
				p := uint64(uintptr(unsafe.Pointer(&n[0])))
				_, ok := pages[p]
				assert(!ok, "page aready exist")
				pages[p] = n
				return p
			},
			del: func(page uint64) {
				_, ok := pages[page]
				assert(ok, "delete non exist page")
				delete(pages, page)
			},
		},
		pages: pages,
		ref:   make(map[string]string),
	}
}

func (c *C) add(key, val string) {
	c.ref[key] = val
	c.tree.Insert([]byte(key), []byte(val))
}

func (c *C) del(key string) bool {
	delete(c.ref, key)
	return c.tree.Delete([]byte(key))
}

func TestBTree(t *testing.T) {
	c := newC()
	c.add("b", strings.Repeat("b", 3000))
	c.add("c", strings.Repeat("c", 3000))
	c.add("a", strings.Repeat("a", 3000))

	c.del("c")
	c.del("b")
	c.del("a")
}
