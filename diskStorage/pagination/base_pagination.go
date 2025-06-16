package pagination

import "github.com/Kush/Database-internals/lib"

type BasePagination interface {
	ReadPage(id PageID) ([]byte, lib.Error)
	WritePage(id PageID, data []byte) lib.Error
	NumPages() int
	AllocatePage() (PageID, lib.Error)
	Close() lib.Error
	Sync() lib.Error
}
