package btree

type BTree struct {
	root uint64
	get  func(page uint64) BNode
	new  func(n BNode) uint64
	del  func(page uint64)
}

func treeInsert(tree *BTree, node BNode, key, val []byte) BNode {
	clone := BNode(make([]byte, 2*BTREE_PAGE_SIZE))
	idx, ok := lookup(node, key)
	switch node.btype() {
	case BNODE_LEAF:
		if ok {
			leafUpdate(clone, node, idx, key, val)
		} else {
			leafInsert(clone, node, idx, key, val)
		}
	case BNODE_NODE:
		kid := treeInsert(tree, tree.get(node.getPtr(idx)), key, val)
		kids := nodeSplit3(kid)
		replaceKids(tree, clone, node, idx, kids)
	}
	return clone
}

func replaceKids(tree *BTree, dst, src BNode, idx uint16, kids []BNode) {
	inc := uint16(len(kids))
	dst.setHeader(BNODE_NODE, src.nkeys()+inc-1)
	appendKvRange(dst, src, 0, 0, idx)
	for i, n := range kids {
		appendKv(dst, idx+uint16(i), tree.new(n), n.getKey(0), nil)
	}
	appendKvRange(dst, src, idx+inc, idx, src.nkeys()-idx-1)
	tree.del(src.getPtr(idx))
}
