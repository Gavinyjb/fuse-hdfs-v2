// Package mfs mirrorfs implementation
package mfs

import (
	"context"
	"fuse-hdfs-v2/hdfslow"
	"io"
	"log"
	"os"
	"sync"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

var _ fs.Node = (*File)(nil)
var _ fs.NodeOpener = (*File)(nil)
var _ fs.HandleReader = (*File)(nil)
var _ fs.HandleWriter = (*File)(nil)
var _ fs.HandleReleaser = (*File)(nil)

// File 表示文件系统中的文件结点
// File既是fs.Node也是fs.Handle
type File struct {
	sync.RWMutex
	attr    fuse.Attr
	path    string
	handler *os.File //用来存放我们打开的文件的句柄
}

func (f *File) Attr(ctx context.Context, o *fuse.Attr) error {
	f.RLock()
	_ = f.readAttr()

	*o = f.attr
	f.RUnlock()
	return nil
}

func (f *File) readAttr() error {
	stats, err := os.Stat(f.path)
	if err != nil {
		//The real file does not exists.
		log.Println("Read attr ERR:", err, f.path)
		return err
	}
	f.attr.Size = uint64(stats.Size())
	f.attr.Mtime = stats.ModTime()
	f.attr.Mode = stats.Mode()

	return nil
}

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	fsHandler, err := os.OpenFile(f.path, int(req.Flags), f.attr.Mode)
	if err != nil {
		log.Print("Open ERR:", err)
		return nil, err
	}

	// 将File对象作为fs.Handle返回
	h := &File{attr: f.attr, path: f.path, handler: fsHandler}
	return h, nil
}

func (f *File) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	return f.handler.Close()
}

func (f *File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	f.RLock()
	defer f.RUnlock()

	if f.handler == nil {
		log.Println("Read: File should be open, aborting request")
		return fuse.ENOTSUP
	}
	bBlockId, flag, err := hdfslow.Path2name(f.path)
	disk := "/dev/sdc"
	if err == nil {
		data, _ := hdfslow.ReadBlk(disk, flag, bBlockId)
		resp.Data = data
		return nil
	} else {
		resp.Data = resp.Data[:req.Size]
		n, err := f.handler.ReadAt(resp.Data, req.Offset)
		if err != nil && err != io.EOF {
			log.Println("Read ERR:", err)
			return err
		}
		resp.Data = resp.Data[:n]

		return nil
	}
}

func (f *File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	f.Lock()
	defer f.Unlock()

	if f.handler == nil {
		log.Println("Write: File should be open, aborting request")
		return fuse.ENOTSUP
	}
	bBlockId, flag, err := hdfslow.Path2name(f.path)
	disk := "/dev/sdc"
	if err == nil {
		//func WriteBlk(disk string, data []byte, bBlockId uint32, writeFlag byte) error
		err := hdfslow.WriteBlk(disk, req.Data, req.Offset, bBlockId, flag)
		if err != nil {
			return err
		}

		temp := []byte(disk)
		temp = append(temp, '\n')
		log.Println("-----", req.Offset, "----")
		n, err := f.handler.WriteAt(temp, 0)
		if err != nil {
			log.Println("Write ERR:", err)
			return err
		}
		resp.Size = n

		return nil
	} else {
		n, err := f.handler.WriteAt(req.Data, req.Offset)
		if err != nil {
			log.Println("Write ERR:", err)
			return err
		}
		resp.Size = n

		return nil
	}

}

func (f *File) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	log.Println("Flushing file", f.path)
	return nil
}
func (f *File) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	log.Println("Fsync call on file", f.path)
	return nil
}
