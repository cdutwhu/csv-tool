package csvtool

import (
	"io"
	"os"

	"github.com/digisan/go-generics/str"
)

// ColumnAttr :
type ColumnAttr struct {
	Idx       int
	Header    string
	IsEmpty   bool
	IsUnique  bool
	HasNull   bool
	HasEmpty  bool
	FilledAll bool // no item is "null/NULL/nil" AND no empty item
}

// Column : header, items, err
func Column(r io.Reader, idx int) (hdr string, items []string, err error) {
	rs, ok := r.(io.ReadSeeker)
	headers, _, err := Info(r)
	if err != nil {
		if ok {
			rs.Seek(0, io.SeekStart)
		}
		return "", nil, err
	}
	if ok {
		defer rs.Seek(0, io.SeekStart)
	}
	if idx >= len(headers) {
		return "", nil, fEf("idx(%d) is out of index range", idx)
	}
	return csvReader(r, func(i, n int, headers, items []string) (ok bool, hdrRow, row string) {
		return true, headers[idx], items[idx]
	}, true, true, nil)
}

// FileColumn : header, items, err
func FileColumn(csvpath string, idx int) (hdr string, items []string, err error) {
	csvFile, err := os.Open(csvpath)
	if err != nil {
		if csvFile != nil {
			csvFile.Close()
		}
		return "", nil, err
	}
	defer csvFile.Close()
	return Column(csvFile, idx)
}

// ColAttr :
func ColAttr(r io.Reader, idx int) (*ColumnAttr, error) {
	rs, ok := r.(io.ReadSeeker)
	hdr, items, err := Column(r, idx)
	if err != nil {
		if ok {
			rs.Seek(0, io.SeekStart)
		}
		return nil, err
	}
	if ok {
		defer rs.Seek(0, io.SeekStart)
	}

	ca := &ColumnAttr{
		Idx:       idx,
		Header:    hdr,
		IsEmpty:   len(items) == 0,
		IsUnique:  len(items) == len(str.MkSet(items...)),
		HasNull:   false,
		HasEmpty:  false,
		FilledAll: true,
	}
	for _, item := range items {
		switch sTrim(item, " \t") {
		case "null", "nil", "NULL":
			ca.HasNull = true
		case "":
			ca.HasEmpty = true
		}
		if ca.HasNull && ca.HasEmpty {
			break
		}
	}
	ca.FilledAll = !ca.HasNull && !ca.HasEmpty
	return ca, nil
}

// FileColAttr :
func FileColAttr(csvpath string, idx int) (*ColumnAttr, error) {
	csvFile, err := os.Open(csvpath)
	if err != nil {
		if csvFile != nil {
			csvFile.Close()
		}
		return nil, err
	}
	defer csvFile.Close()
	return ColAttr(csvFile, idx)
}
