package btree

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestSetHeader(t *testing.T) {
	node := BNode(make([]byte, BTREE_PAGE_SIZE))
	node.setHeader(BNODE_LEAF, 2)
	if !bytes.Equal(node[:4], []byte{0x01, 0x00, 0x02, 0x00}) {
		t.Fatal("header should be leaf with nkeys equal 2")
	}
}

func TestGetHeader(t *testing.T) {
	node := BNode(make([]byte, BTREE_PAGE_SIZE))
	copy(node, []byte{0x01, 0x00, 0x02, 0x00})

	if node.btype() != BNODE_LEAF {
		t.Fatal("node shuld be leaf")
	}
	if node.nkeys() != 2 {
		t.Fatal("nkeys should equal 2")
	}
}

func TestSetPtr(t *testing.T) {
	node := BNode(make([]byte, BTREE_PAGE_SIZE))
	node.setHeader(BNODE_LEAF, 4)
	node.setPtr(0, 111)
	if binary.LittleEndian.Uint64(node[4:]) != 111 {
		t.Fatal("set pointer")
	}
}

func TestGetPtr(t *testing.T) {
	node := BNode(make([]byte, BTREE_PAGE_SIZE))
	node.setHeader(BNODE_LEAF, 4)
	binary.LittleEndian.PutUint64(node[4+0*8:], 111)
	binary.LittleEndian.PutUint64(node[4+1*8:], 222)
	binary.LittleEndian.PutUint64(node[4+2*8:], 333)
	binary.LittleEndian.PutUint64(node[4+3*8:], 444)
	if node.getPtr(0) != 111 {
		t.Fatal("index 0")
	}
	if node.getPtr(1) != 222 {
		t.Fatal("index 1")
	}
	if node.getPtr(2) != 333 {
		t.Fatal("index 2")
	}
	if node.getPtr(3) != 444 {
		t.Fatal("index 3")
	}
}

func TestAppendKv(t *testing.T) {
	node := BNode(make([]byte, BTREE_PAGE_SIZE))
	node.setHeader(BNODE_LEAF, 4)
	tests := []struct {
		ptr uint64
		key string
		val string
	}{
		{111, "a", "hello"},
		{222, "b", "hi"},
		{333, "c", "howdy"},
		{444, "d", "sawasdee"},
	}

	for i, test := range tests {
		appendKv(node, uint16(i), test.ptr, []byte(test.key), []byte(test.val))
	}
	for i, test := range tests {
		if node.getPtr(uint16(i)) != test.ptr {
			t.Fatalf("get ptr at idx:%d", i)
		}
		if string(node.getKey(uint16(i))) != test.key {
			t.Fatalf("get key at idx:%d", i)
		}
		if string(node.getVal(uint16(i))) != test.val {
			t.Fatalf("get val at idx:%d", i)
		}
	}
}

func TestLookup(t *testing.T) {
	node := BNode(make([]byte, BTREE_PAGE_SIZE))
	node.setHeader(BNODE_NODE, 3)
	appendKv(node, 0, 111, []byte("b"), nil)
	appendKv(node, 1, 222, []byte("d"), nil)
	appendKv(node, 2, 333, []byte("f"), nil)
	assert(node.nkeys() == 3, "")

	type expect struct {
		idx   uint16
		found bool
	}
	tests := []struct {
		key    string
		expect expect
	}{
		{"b", expect{0, true}},
		{"d", expect{1, true}},
		{"f", expect{2, true}},
		{"a", expect{0, false}},
		{"c", expect{1, false}},
		{"e", expect{2, false}},
		{"g", expect{3, false}},
	}
	for i, test := range tests {
		idx, found := lookup(node, []byte(test.key))
		if idx != test.expect.idx {
			t.Fatalf("test:%d expect idx:%d got:%d", i, test.expect.idx, idx)
		}
		if found != test.expect.found {
			t.Fatalf("test:%d expect found:%t got:%t", i, test.expect.found, found)
		}
	}
}
