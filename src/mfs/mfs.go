// Package mfs mirrorfs implementation
package mfs

import (
	"context"
	"log"
	"os"
	"sync/atomic"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

// MirrorFS 表示根文件系统
type MirrorFS struct {
	root   *Dir // 根目录
	nodeId uint64
	path   string //mirror path
}

// Compile-time interface checks.
var _ fs.FS = (*MirrorFS)(nil)
var _ fs.FSStatfser = (*MirrorFS)(nil)

const DefMode = os.FileMode(0777)

func NewMirrorFS(path string) *MirrorFS {
	fs := &MirrorFS{
		path: path,
	}
	fs.root = fs.newDir(path, os.ModeDir|DefMode)
	if fs.root.attr.Inode != 1 {
		panic("Root node should have been assigned id 1")
	}
	return fs
}

func (mf *MirrorFS) nextId() uint64 {
	return atomic.AddUint64(&mf.nodeId, 1)
}

func (mf *MirrorFS) newDir(path string, mode os.FileMode) *Dir {
	n := time.Now()
	return &Dir{
		attr: fuse.Attr{
			Inode:  mf.nextId(),
			Atime:  n,
			Mtime:  n,
			Ctime:  n,
			Crtime: n,
			Mode:   os.ModeDir | mode,
		},
		path: path,
		fs:   mf,
	}
}

func (mf *MirrorFS) newFile(path string, mode os.FileMode) *File {
	n := time.Now()
	return &File{
		attr: fuse.Attr{
			Inode:  mf.nextId(),
			Atime:  n,
			Mtime:  n,
			Ctime:  n,
			Crtime: n,
			Mode:   mode,
		},
		path: path,
	}
}

func (mf *MirrorFS) Root() (fs.Node, error) {
	return mf.root, nil
}

func (mf *MirrorFS) Statfs(ctx context.Context, req *fuse.StatfsRequest, res *fuse.StatfsResponse) error {
	s := syscall.Statfs_t{}
	err := syscall.Statfs(mf.path, &s)
	if err != nil {
		log.Println("DRIVE | Statfs syscall failed:", err)
		return err
	}

	res.Blocks = s.Blocks
	res.Bfree = s.Bfree
	res.Bavail = s.Bavail
	res.Ffree = s.Ffree
	res.Bsize = uint32(s.Bsize)

	return nil
}
