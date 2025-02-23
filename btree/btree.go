package btree

import (
	"encoding/binary"
	"fmt"
)

/*
 *	| type | nkeys |  pointers  |  offsets   | key-values | unused |
 *  |  2B  |   2B  | nkeys × 8B | nkeys × 2B |     ...    |        |
 *
 *
 *  | key_size | val_size | key | val |
 *  |    2B    |    2B    | ... | ... |
 */

const (
	BNODE_NODE = iota
	BNODE_LEAF
)

const BTREE_PAGE_SIZE = 4096
const BTREE_MAX_KEY_SIZE = 1000
const BTREE_MAX_VAL_SIZE = 3000

func assert(b bool, msg string) {
	if !b {
		panic(fmt.Errorf("assert: %s", msg))
	}
}

func init() {
	leafNodeMax := 4 + 1*8 + 1*2 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	internalNodeMax := 4 + 2*8 + 2*2 + 2*BTREE_MAX_KEY_SIZE
	assert(leafNodeMax <= BTREE_PAGE_SIZE, "data cannot fit in leaf node")
	assert(internalNodeMax <= BTREE_PAGE_SIZE, "internal node should hold at least 2 keys")
}

type BNode []byte

func (n BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(n[:2])
}

func (n BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(n[2:4])
}

func (n BNode) setHeader(btype, nkeys uint16) {
	binary.LittleEndian.PutUint16(n[:2], btype)
	binary.LittleEndian.PutUint16(n[2:4], nkeys)
}

func (n BNode) getPtr(idx uint16) uint64 {
	checkIdx(idx, n.nkeys())
	return binary.LittleEndian.Uint64(n[4+idx*8:])
}

func (n BNode) setPtr(idx uint16, v uint64) {
	checkIdx(idx, n.nkeys())
	binary.LittleEndian.PutUint64(n[4+idx*8:], v)
}

func (n BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	checkIdx(idx, n.nkeys()+1)
	pos := 4 + 8*n.nkeys() + 2*(idx-1)
	return binary.LittleEndian.Uint16(n[pos:])
}

func (n BNode) setOffset(idx uint16, offset uint16) {
	checkIdx(idx, n.nkeys()+1)
	pos := 4 + 8*n.nkeys() + 2*(idx-1)
	binary.LittleEndian.PutUint16(n[pos:], offset)
}

func (n BNode) kvPos(idx uint16) uint16 {
	nkeys := n.nkeys()
	return 4 + 8*nkeys + 2*nkeys + n.getOffset(idx)
}

func (n BNode) getKey(idx uint16) []byte {
	p := n.kvPos(idx)
	klen := binary.LittleEndian.Uint16(n[p:])
	return n[p+4:][:klen]
}

func (n BNode) getVal(idx uint16) []byte {
	p := n.kvPos(idx)
	klen := binary.LittleEndian.Uint16(n[p:])
	vlen := binary.LittleEndian.Uint16(n[p+2:])
	return n[p+4+klen:][:vlen]
}

func (n BNode) nbytes() uint16 {
	return n.getOffset(n.nkeys())
}

func appendKv(n BNode, idx uint16, ptr uint64, key []byte, val []byte) {
	n.setPtr(idx, ptr)
	pos := n.kvPos(idx)
	binary.LittleEndian.PutUint16(n[pos:], uint16(len(key)))
	binary.LittleEndian.PutUint16(n[pos+2:], uint16(len(val)))
	copy(n[pos+4:], key)
	copy(n[int(pos)+4+len(key):], val)
	n.setOffset(idx+1, n.getOffset(idx)+4+uint16(len(key)+len(val)))
}

func checkIdx(idx, length uint16) {
	assert(idx < length, fmt.Sprintf("index out of bound i=%d, len=%d", idx, length))
}
