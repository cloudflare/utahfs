package persistent

import (
	"testing"
)

func TestParentStep(t *testing.T) {
	checkParents := func(node uint64, parents ...uint64) {
		for _, parent := range parents {
			node = parentStep(node)
			if node != parent {
				t.Fatalf("unexpected parent node returned: got=%v, expected=%v", node, parent)
			}
		}
	}

	checkParents(0, 1, 3, 7)
	checkParents(2, 1, 3, 7)
	checkParents(4, 5, 3, 7)
	checkParents(6, 5, 3, 7)
	checkParents(8, 9, 11, 7)
	checkParents(10, 9, 11, 7)
	checkParents(12, 13, 11, 7)
}

func TestRootNode(t *testing.T) {
	checkRoot := func(leaves, expected uint64) {
		if cand := rootNode(leaves); cand != expected {
			t.Fatalf("unexpected root node returned: got=%v, expected=%v", cand, expected)
		}
	}

	checkRoot(2, 1)
	checkRoot(10, 15)
	checkRoot(7, 7)
}
