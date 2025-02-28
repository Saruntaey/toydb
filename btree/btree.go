package btree

type BTree struct {
	root uint64
	get  func(page uint64) BNode
	new  func(n BNode) uint64
	del  func(page uint64)
}

func (t *BTree) Insert(key, val []byte) {
	assert(len(key) != 0, "empty key")
	assert(len(key) <= BTREE_MAX_KEY_SIZE, "key too big")
	assert(len(val) <= BTREE_MAX_VAL_SIZE, "val too big")

	if t.root == 0 {
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		root.setHeader(BNODE_LEAF, 1)
		appendKv(root, 0, 0, key, val)
		t.root = t.new(root)
		return
	}
	node := t.get(t.root)
	defer t.del(t.root)
	node = treeInsert(t, node, key, val)
	nodes := nodeSplit3(node)
	nsplit := uint16(len(nodes))
	if nsplit == 1 {
		t.root = t.new(nodes[0])
		return
	}
	root := BNode(make([]byte, BTREE_PAGE_SIZE))
	root.setHeader(BNODE_NODE, nsplit)
	for i, n := range nodes {
		appendKv(root, uint16(i), t.new(n), n.getKey(0), nil)
	}
	t.root = t.new(root)
}

func (t *BTree) Delete(key []byte) bool {
	assert(len(key) != 0, "empty key")
	assert(len(key) <= BTREE_MAX_KEY_SIZE, "key too big")

	if t.root == 0 {
		return false
	}
	node := t.get(t.root)
	node, ok := treeDelete(t, node, key)
	if !ok {
		return false
	}
	defer t.del(t.root)

	switch {
	case node.nkeys() == 0:
		t.root = 0
		return true
	case node.nkeys() == 1 && node.btype() == BNODE_NODE:
		t.root = node.getPtr(0)
		return true
	default:
		t.root = t.new(node)
		return true
	}
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
		if !ok && idx > 0 {
			idx--
		}
		kid := treeInsert(tree, tree.get(node.getPtr(idx)), key, val)
		kids := nodeSplit3(kid)
		nodeReplaceKidN(tree, clone, node, idx, kids)
	default:
		panic("bad node")
	}
	return clone
}

func nodeReplaceKidN(tree *BTree, dst, src BNode, idx uint16, kids []BNode) {
	inc := uint16(len(kids))
	dst.setHeader(BNODE_NODE, src.nkeys()+inc-1)
	appendKvRange(dst, src, 0, 0, idx)
	for i, n := range kids {
		appendKv(dst, idx+uint16(i), tree.new(n), n.getKey(0), nil)
	}
	appendKvRange(dst, src, idx+inc, idx+1, src.nkeys()-idx-1)
	tree.del(src.getPtr(idx))
}

func nodeReplace2Kid(tree *BTree, dst, src BNode, idx uint16, kid BNode) {
	dst.setHeader(BNODE_NODE, src.nkeys()-1)
	appendKvRange(dst, src, 0, 0, idx)
	appendKv(dst, idx, tree.new(kid), kid.getKey(0), nil)
	appendKvRange(dst, src, idx+1, idx+2, src.nkeys()-idx-2)
	tree.del(src.getPtr(idx))
	tree.del(src.getPtr(idx + 1))
}

func shouldMerge(tree *BTree, parent, updated BNode, idx uint16) (int, BNode) {
	if updated.nbytes() > BTREE_PAGE_SIZE/4 {
		return 0, BNode{}
	}
	if idx > 0 {
		leftSibling := tree.get(parent.getPtr(idx - 1))
		if leftSibling.nbytes()+updated.nbytes()-4 <= BTREE_PAGE_SIZE {
			return -1, leftSibling
		}
	}
	if idx < parent.nkeys()-1 {
		rightSibling := tree.get(parent.getPtr(idx + 1))
		if rightSibling.nbytes()+updated.btype()-4 <= BTREE_PAGE_SIZE {
			return 1, rightSibling
		}
	}
	return 0, BNode{}
}

func treeDelete(tree *BTree, node BNode, key []byte) (BNode, bool) {
	var updated BNode

	idx, ok := lookup(node, key)
	switch node.btype() {
	case BNODE_LEAF:
		if !ok {
			return BNode{}, false
		}
		updated = BNode(make([]byte, BTREE_PAGE_SIZE))
		leafDelete(updated, node, idx)
		return updated, true
	case BNODE_NODE:
		if !ok && idx > 0 {
			idx--
		}
		kid, ok := treeDelete(tree, tree.get(node.getPtr(idx)), key)
		if !ok {
			return updated, false
		}
		updated = BNode(make([]byte, BTREE_PAGE_SIZE))
		dir, sibling := shouldMerge(tree, node, kid, idx)
		switch {
		case dir == -1:
			merged := BNode(make([]byte, BTREE_PAGE_SIZE))
			nodeMerge(merged, sibling, updated)
			nodeReplace2Kid(tree, updated, node, idx-1, merged)
		case dir == 1:
			merged := BNode(make([]byte, BTREE_PAGE_SIZE))
			nodeMerge(merged, updated, sibling)
			nodeReplace2Kid(tree, updated, node, idx, merged)
		case dir == 0 && kid.nkeys() == 0: // kid is empty but have no sibling to merge
			assert(node.nkeys() == 1 && idx == 0, "node should have one key")
			updated.setHeader(BNODE_NODE, 0)
		case dir == 0 && kid.nkeys() > 0:
			nodeReplaceKidN(tree, updated, node, idx, []BNode{kid})
		}
		return updated, true
	default:
		panic("bad node")
	}
}
