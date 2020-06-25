package utahfs

import (
	"bytes"
	"context"
	"fmt"

	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
)

type archive struct {
	*filesystem
}

// NewArchive wraps NewFilesystem but refuses to delete or overwrite data.
//
// It allows new files to be created, and old files to be moved / renamed /
// appended to. Empty directories may be deleted, but no files may be deleted or
// overwritten. This is just enforced by the FUSE binding, not by an actual
// access management system. Data stored is compatible with NewFilesystem.
func NewArchive(bfs *BlockFilesystem) (fuseutil.FileSystem, error) {
	fs, err := NewFilesystem(bfs)
	if err != nil {
		return nil, err
	}
	return archive{fs.(*filesystem)}, nil
}

func (a archive) SetInodeAttributes(ctx context.Context, op *fuseops.SetInodeAttributesOp) error {
	return a.setInodeAttributes(ctx, op, true)
}

func (a archive) Rename(ctx context.Context, op *fuseops.RenameOp) error {
	return a.rename(ctx, op, true)
}

func (a archive) Unlink(ctx context.Context, op *fuseops.UnlinkOp) error {
	return a.unlink(ctx, op, true)
}

func (a archive) WriteFile(ctx context.Context, op *fuseops.WriteFileOp) error {
	return a.writeFile(ctx, op, true)
}

// checkForChanges ensures that `op` won't modify any already-written parts of
// the file stored by `nd`.
func checkForChanges(nd *node, op *fuseops.WriteFileOp) error {
	max := min(
		op.Offset+int64(len(op.Data)),
		int64(nd.Attrs.Size),
	)

	pos := op.Offset
	for pos < max {
		temp := make([]byte, min(32*1024, max-pos))
		n, err := nd.ReadAt(temp, pos)
		if err != nil {
			return fmt.Errorf("utahfs: failed to check if operation modifies file: %v", err)
		}
		cand := op.Data[pos-op.Offset : pos+int64(n)-op.Offset]
		if !bytes.Equal(cand, temp[:n]) {
			return fmt.Errorf("utahfs: refusing to admit write that would modify file contents")
		}
		pos += int64(n)
	}

	return nil
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
