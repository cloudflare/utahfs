package main

import (
	"os"

	"github.com/Bren2010/utahfs"
	"github.com/billziss-gh/cgofuse/fuse"
)

func main() {
	fs := utahfs.New()

	host := fuse.NewFileSystemHost(fs)
	host.SetCapReaddirPlus(true)
	host.Mount("", os.Args[1:])
}
