package pagination

import (
	"fmt"
	"os"

	"github.com/Kush/Database-internals/lib"
)

const PageSize = 4096

type PageID uint32

type Pager struct {
	file     *os.File
	pageSize int
}

func NewPager(path string) (*Pager, lib.Error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, lib.EmptyError().AddErr(lib.PaginationError, fmt.Errorf("failed to open file %s: %w", path, err))
	}
	return &Pager{
		file:     file,
		pageSize: PageSize,
	}, lib.EmptyError()
}

func (p *Pager) ReadPage(id PageID) ([]byte, lib.Error) {
	buf := make([]byte, p.pageSize)
	offset := int64(id) * int64(p.pageSize)
	_, err := p.file.ReadAt(buf, offset)
	if err != nil {
		return nil, lib.EmptyError().AddErr(lib.PaginationError, fmt.Errorf("failed to read page %d: %w", id, err))
	}
	return buf, lib.EmptyError()
}

func (p *Pager) WritePage(id PageID, data []byte) lib.Error {
	// if len(data) != p.pageSize {
	// 	return lib.EmptyError().AddErr(lib.InvalidInputError, fmt.Errorf("write page: data length %d does not match page size %d", len(data), p.pageSize))
	// }
	offset := int64(id) * int64(p.pageSize)
	_, err := p.file.WriteAt(data, offset)
	if err != nil {
		return lib.EmptyError().AddErr(lib.PaginationError, fmt.Errorf("failed to write page %d: %w", id, err))
	}
	return lib.EmptyError()
}

func (p *Pager) NumPages() int {
	info, err := p.file.Stat()
	if err != nil {
		lib.EmptyError().AddWarning(lib.PaginationError, fmt.Errorf("failed to get file info: %w", err))
		return 0
	}
	return int(info.Size() / int64(p.pageSize))
}

func (p *Pager) AllocatePage() (PageID, lib.Error) {
	currentNumPages := p.NumPages()

	newPageID := PageID(currentNumPages)
	page := make([]byte, p.pageSize)
	if err := p.WritePage(newPageID, page); err.IsNotEmpty() {
		return 0, err
	}

	return newPageID, lib.EmptyError()
}


func (p *Pager) Close() lib.Error {
	if err := p.file.Close(); err != nil {
		return lib.EmptyError().AddErr(lib.PaginationError, fmt.Errorf("failed to close main file: %w", err))
	}
	
	return lib.EmptyError()
}

func (p *Pager) Sync() lib.Error {
	if err := p.file.Sync(); err != nil {
		return lib.EmptyError().AddErr(lib.PaginationError, fmt.Errorf("failed to sync main file: %w", err))
	}

	return lib.EmptyError()
}
