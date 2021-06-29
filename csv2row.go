package csvtool

import (
	"bytes"
	"encoding/csv"
	"io"
	"os"
	"path/filepath"
)

func mkValid(item string) string {
	if len(item) > 1 {
		hasComma, hasQuote, hasLF := sContains(item, ","), sContains(item[1:len(item)-1], "\""), sContains(item, "\n")
		if hasComma || hasQuote || hasLF {
			if hasQuote {
				item = sReplaceAll(item, "\"", "\"\"")
			}
			if item[0] != '"' || item[len(item)-1] != '"' {
				item = "\"" + item + "\""
			}
		}
	}
	return item
}

// Info : headers, nItem, error
func Info(r io.Reader) ([]string, int, error) {
	rs, ok := r.(io.ReadSeeker)
	content, err := csv.NewReader(r).ReadAll()
	if err != nil {
		if ok {
			rs.Seek(0, io.SeekStart)
		}
		return nil, -1, err
	}
	if ok {
		defer rs.Seek(0, io.SeekStart)
	}
	if len(content) == 0 {
		return []string{}, 0, nil
	}
	return content[0], len(content) - 1, nil
}

// FileInfo : headers, nItem, error
func FileInfo(csvpath string) ([]string, int, error) {
	csvFile, err := os.Open(csvpath)
	if err != nil {
		if csvFile != nil {
			csvFile.Close()
		}
		return nil, 0, err
	}
	defer csvFile.Close()
	return Info(csvFile)
}

// ScanByRow : if [f arg: i==-1], it is pure HeaderRow csv
func ScanByRow(in []byte, f func(i, n int, headers, items []string) (ok bool, hdrRow, row string), keepHdrOnEmpty bool, w io.Writer) (string, []string, error) {
	return csvReader(bytes.NewReader(in), f, keepHdrOnEmpty, false, w)
}

// csvReader : if [f arg: i==-1], it is pure HeaderRow csv
func csvReader(r io.Reader, f func(i, n int, headers, items []string) (ok bool, hdrRow, row string), keepHdrOnEmpty, keepAnyRow bool, w io.Writer) (string, []string, error) {
	rs, ok := r.(io.ReadSeeker)
	content, err := csv.NewReader(r).ReadAll()
	// failOnErr("%v", err)
	if err != nil {
		if ok {
			rs.Seek(0, io.SeekStart)
		}
		return "", nil, err
	}
	if ok {
		defer rs.Seek(0, io.SeekStart)
	}

	if len(content) < 1 {
		return "", []string{}, fEf("FILE_EMPTY")
	}

	headers := make([]string, 0)
	for i, hdrItem := range content[0] {
		if hdrItem == "" {
			hdrItem = fSf("column_%d", i)
			warnOnErr("%v: - column[%d] is empty, mark [%s]", fEf("CSV_COLUMN_HEADER_EMPTY"), i, hdrItem)
		}
		headers = append(headers, mkValid(hdrItem))
	}

	hdrOnly := false
	if len(content) == 1 {
		hdrOnly = true
	}

	// Remove The Header Row --------------
	content = content[1:]

	// check
	N := len(content) // N is row's count
	hdrRow, allRows := "", []string{}

	if keepHdrOnEmpty && N == 0 {
		hdrRow = sJoin(headers, ",")
	}

	// if no f provided, we select all rows //
	if f == nil {
		hdrRow = sJoin(headers, ",")
		if hdrOnly {
			allRows = []string{""} // hdrOnly, allRows all are empty
		} else {
			for _, d := range content {
				allRows = append(allRows, sJoin(d, ","))
			}
		}
		goto SAVE
	}

	if hdrOnly {
		if ok, hRow, _ := f(-1, 1, headers, []string{}); ok {
			hdrRow = hRow
			allRows = []string{""} // hdrOnly, allRows all are empty
		}
	}

	for i, d := range content {
		if ok, hRow, row := f(i, N, headers, d); ok {
			hdrRow = hRow
			if keepAnyRow {
				allRows = append(allRows, row)
			} else {
				if row != "" { // we use f to return row content for deciding wether to add this row
					allRows = append(allRows, row)
				}
			}
		}
	}

SAVE:
	// save
	if w != nil {
		csvbytes := []byte(sTrimSuffix(hdrRow+"\n"+sJoin(allRows, "\n"), "\n"))
		_, err = w.Write(csvbytes)
		failP1OnErr("%v", err)
	}

	return hdrRow, allRows, nil
}

// File2Rows :
func File2Rows(csvpath string, f func(i, n int, headers, items []string) (ok bool, hdrRow, row string), keepHdrOnEmpty bool, outcsv string) (string, []string, error) {

	fr, err := os.Open(csvpath)
	failP1OnErr("csvpath: he file is not found || wrong root : %v", err)
	defer fr.Close()

	mustCreateDir(filepath.Dir(outcsv))
	fw, err := os.OpenFile(outcsv, os.O_WRONLY|os.O_CREATE, 0666)
	failP1OnErr("outcsv: The file is not found || wrong root : %v", err)
	defer fw.Close()

	hRow, rows, err := csvReader(fr, f, keepHdrOnEmpty, false, fw)
	failOnErrWhen(rows == nil, "%v @ %s", err, csvpath) // go internal csv func error
	return hRow, rows, err
}
