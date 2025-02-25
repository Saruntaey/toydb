package btree

import (
	"bytes"
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
	leafNodeMax := 4 + 1*8 + 1*2 + (4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE)
	internalNodeMax := 4 + 2*8 + 2*2 + 2*(4+BTREE_MAX_KEY_SIZE)
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

func appendKvRange(dst, src BNode, dstStart, srcStart, n uint16) {
	for i := uint16(0); i < n; i++ {
		dstPos, srcPos := dstStart+i, srcStart+i
		appendKv(dst, dstPos, src.getPtr(srcPos), src.getKey(srcPos), src.getVal(srcPos))
	}
}

func leafInsert(dst, src BNode, idx uint16, key, val []byte) {
	dst.setHeader(BNODE_LEAF, src.nkeys()+1)
	appendKvRange(dst, src, 0, 0, idx)
	appendKv(dst, idx, 0, key, val)
	appendKvRange(dst, src, idx+1, idx, src.nkeys()-idx)
}

func leafUpdate(dst, src BNode, idx uint16, key, val []byte) {
	dst.setHeader(BNODE_LEAF, src.nkeys())
	appendKvRange(dst, src, 0, 0, idx)
	appendKv(dst, idx, 0, key, val)
	appendKvRange(dst, src, idx+1, idx+1, src.nkeys()-idx-1)
}

func lookup(n BNode, key []byte) (uint16, bool) {
	i := uint16(0)
	j := n.nkeys() - 1
	var mid uint16

	for i <= j {
		mid = i + (j-i)/2
		if diff := bytes.Compare(n.getKey(mid), key); diff == 0 {
			return mid, true
		} else if diff < 0 {
			i = mid + 1
		} else {
			j = mid - 1
		}
	}
	return i, false
}

func nodeSplit2(left, right, src BNode) {
	assert(src.nkeys() >= 2, "nodeSplit2: split node with one key")
	leftSize := func(idx uint16) uint16 {
		return 4 + 8*idx + 2*idx + src.getOffset(idx)
	}
	l := uint16(0)
	r := src.nkeys() - 1
	for l <= r {
		mid := l + (r-l)/2
		if leftSize(mid) <= BTREE_PAGE_SIZE {
			l = mid + 1
		} else {
			r = mid - 1
		}
	}
	nleft := r
	assert(nleft < src.nkeys(), "nodeSplit2: do not need to split")
	nright := src.nkeys() - nleft

	left.setHeader(src.btype(), nleft)
	right.setHeader(src.btype(), nright)
	appendKvRange(left, src, 0, 0, nleft)
	appendKvRange(right, src, 0, nleft, nright)
	assert(left.nbytes() <= BTREE_PAGE_SIZE, "nodeSplit2: left node too big")
}

func nodeSplit3(src BNode) []BNode {
	if src.nbytes() <= BTREE_PAGE_SIZE {
		return []BNode{src}
	}
	left := make(BNode, BTREE_PAGE_SIZE)
	right := make(BNode, 2*BTREE_PAGE_SIZE)
	nodeSplit2(left, right, src)
	if right.nbytes() <= BTREE_PAGE_SIZE {
		return []BNode{left, right}
	}
	rightLeft := make(BNode, BTREE_PAGE_SIZE)
	rightRight := make(BNode, BTREE_PAGE_SIZE)
	nodeSplit2(rightLeft, rightRight, right)
	assert(rightRight.nbytes() <= BTREE_PAGE_SIZE, "nodeSplit3: rightRight node too big")
	return []BNode{left, rightLeft, rightRight}
}

func leafDelete(dst, src BNode, idx uint16) {
	dst.setHeader(BNODE_LEAF, src.nkeys()-1)
	appendKvRange(dst, src, 0, 0, idx)
	appendKvRange(dst, src, idx, idx+1, src.nkeys()-idx-1)
}

func nodeMerge(dst, left, right BNode) {
	dst.setHeader(left.nbytes(), left.nkeys()+right.nkeys())
	appendKvRange(dst, left, 0, 0, left.nkeys())
	appendKvRange(dst, right, left.nkeys(), 0, right.nkeys())
}

func checkIdx(idx, length uint16) {
	assert(0 <= idx && idx < length, fmt.Sprintf("index out of bound i=%d, len=%d", idx, length))
}
