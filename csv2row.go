package csvtool

import (
	"encoding/csv"
	"io"
	"os"
)

// Info :
func Info(r io.Reader) (string, int, error) {
	content, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return "", -1, err
	}
	return sJoin(content[0], ","), len(content) - 1, nil
}

// FileInfo :
func FileInfo(csvpath string) (string, int, error) {
	csvFile, err := os.Open(csvpath)
	failP1OnErr("The file is not found || wrong root : %v", err)
	defer csvFile.Close()
	return Info(csvFile)
}

// ReaderByRow :
func ReaderByRow(r io.Reader, f func(i, n int, headers, items []string) (ok bool, hdrRow, row string), outcsv string) (string, []string, error) {
	content, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return "", nil, err
	}
	if len(content) < 1 {
		return "", []string{}, fEf("FILE_EMPTY")
	}

	headers := make([]string, 0)
	for i, headE := range content[0] {
		if headE == "" {
			headE = fSf("column_%d", i)
			fPln(warnOnErr("%v: - column[%d] is empty, mark [%s]", fEf("CSV_COLUMN_HEADER_EMPTY"), i, headE))
		}
		headers = append(headers, headE)
	}

	// Remove The Header Row
	content = content[1:]

	// check
	N := len(content) // N is row's count
	hdrRow, allRows := "", []string{}
	for i, d := range content {
		if ok, hRow, row := f(i, N, headers, d); ok {
			hdrRow = hRow
			if row != "" {
				allRows = append(allRows, row)
			}
		}
	}

	// save
	if outcsv != "" {
		outcsv = sTrimSuffix(outcsv, ".csv") + ".csv"
		mustWriteFile(outcsv, []byte(sTrimSuffix(hdrRow+"\n"+sJoin(allRows, "\n"), "\n")))
	}

	return hdrRow, allRows, nil
}

// File2Rows :
func File2Rows(csvpath string, f func(i, n int, headers, items []string) (ok bool, hdrRow, row string), outcsv string) (string, string, error) {
	csvFile, err := os.Open(csvpath)
	failP1OnErr("The file is not found || wrong root : %v", err)
	defer csvFile.Close()
	hRow, rows, err := ReaderByRow(csvFile, f, outcsv)
	return hRow, sJoin(rows, "\n"), err
}

// SubFile : content iRow start from 0. i.e. 1st content row index is 0
func SubFile(csvpath string, incColMode bool, hdrNames []string, incRowMode bool, iRows []int, outcsv string) (string, string, error) {

	fnCol, fnRow := notexist, notexist
	if incColMode {
		fnCol = exist
	}
	if incRowMode {
		fnRow = exist
	}

	return File2Rows(csvpath, func(idx, cnt int, headers, items []string) (bool, string, string) {

		// select needed columns
		gHdrNames := cvt2GSlc(hdrNames)
		cIdxGrp := []interface{}{}
		for i, header := range headers {
			switch {
			case fnCol(header, gHdrNames...):
				cIdxGrp = append(cIdxGrp, i)
			}
		}

		// filter columns headers
		hdrLeft := []string{}
		for i, header := range headers {
			if exist(i, cIdxGrp...) {
				if sContains(header, ",") {
					header = "\"" + header + "\""
				}
				hdrLeft = append(hdrLeft, header)
			}
		}
		hdrRow := sJoin(hdrLeft, ",")

		// filter column items
		itemLeft := []string{}
		for i, item := range items {
			if exist(i, cIdxGrp...) {
				if sContains(item, ",") {
					item = "\"" + item + "\""
				}
				itemLeft = append(itemLeft, item)
			}
		}
		itemRow := sJoin(itemLeft, ",")

		// select needed rows
		if fnRow(idx, cvt2GSlc(iRows)...) {
			return true, hdrRow, itemRow
		}

		return true, hdrRow, "" // still "ok" as hdrRow is needed even if empty content

	}, outcsv)
}

// SelFile :
// func SelFile(csvpath string, andMode bool, conditions []struct{ header, value string }, outcsv string) (string, string, error) {

// 	return File2Rows(csvpath, func(i int, headers, items []string, line string) (bool, string, string) {

// 		for _, cond := range conditions {
// 			idx := -1
// 			for i, hdr := range headers {
// 				if hdr == cond.header {
// 					idx = i
// 					break
// 				}
// 			}

// 			//
// 			if idx != -1 {
// 				if items[idx] == cond.value {

// 				}
// 			}

// 		}

// 	}, outcsv)
// }
