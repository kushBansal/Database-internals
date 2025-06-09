package pagination

import "github.com/Kush/Database-internals/lib"

type BasePagination interface {
	ReadPage(id PageID) ([]byte, lib.Error)
	WritePage(id PageID, data []byte) lib.Error
}
