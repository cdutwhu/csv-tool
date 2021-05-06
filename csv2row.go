package csvtool

import (
	"encoding/csv"
	"io"
	"os"
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
	content, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, -1, err
	}
	return content[0], len(content) - 1, nil
}

// FileInfo : headers, nItem, error
func FileInfo(csvpath string) ([]string, int, error) {
	csvFile, err := os.Open(csvpath)
	failP1OnErr("The file is not found || wrong root : %v", err)
	defer csvFile.Close()
	return Info(csvFile)
}

// ReaderByRow : if [f arg: i==-1], it is pure HeaderRow csv
func ReaderByRow(r io.Reader, f func(i, n int, headers, items []string) (ok bool, hdrRow, row string), oriHdrIfNoRows bool, outcsv string) (string, []string, error) {
	content, err := csv.NewReader(r).ReadAll()
	// failOnErr("%v", err)
	if err != nil {
		return "", nil, err
	}

	if len(content) < 1 {
		return "", []string{}, fEf("FILE_EMPTY")
	}

	headers := make([]string, 0)
	for i, hdrItem := range content[0] {
		if hdrItem == "" {
			hdrItem = fSf("column_%d", i)
			fPln(warnOnErr("%v: - column[%d] is empty, mark [%s]", fEf("CSV_COLUMN_HEADER_EMPTY"), i, hdrItem))
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

	if oriHdrIfNoRows && N == 0 {
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
			if row != "" { // we use f to return row content for deciding wether to add this row
				allRows = append(allRows, row)
			}
		}
	}

SAVE:
	// save
	if outcsv != "" {
		outcsv = sTrimSuffix(outcsv, ".csv") + ".csv"
		mustWriteFile(outcsv, []byte(sTrimSuffix(hdrRow+"\n"+sJoin(allRows, "\n"), "\n")))
	}

	return hdrRow, allRows, nil
}

// File2Rows :
func File2Rows(csvpath string, f func(i, n int, headers, items []string) (ok bool, hdrRow, row string), oriHdrIfNoRows bool, outcsv string) (string, []string, error) {
	csvFile, err := os.Open(csvpath)
	failP1OnErr("The file is not found || wrong root : %v", err)
	defer csvFile.Close()
	hRow, rows, err := ReaderByRow(csvFile, f, oriHdrIfNoRows, outcsv)
	failOnErrWhen(rows == nil, "%v @ %s", err, csvpath) // go internal csv func error
	return hRow, rows, err
}
