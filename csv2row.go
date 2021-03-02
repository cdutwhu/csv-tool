package csvtool

import (
	"encoding/csv"
	"io"
	"os"
)

// ReaderByRow :
func ReaderByRow(r io.Reader, f func(i int, headers, items []string, line string) (ok bool, headerline, rowline string), outcsv string) (string, string, error) {
	content, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return "", "", err
	}
	if len(content) < 1 {
		return "", "", fEf("FILE_EMPTY")
	}

	headers := make([]string, 0)
	for i, headE := range content[0] {
		if headE == "" {
			headE = fSf("column_%d", i)
			fPln(warnOnErr("%v: - column[%d] is empty, mark [%s]", fEf("CSV_COLUMN_HEADER_EMPTY"), i, headE))
		}
		headers = append(headers, headE)
	}

	// Remove the header row
	content = content[1:]

	headerLine := ""
	allLines := []string{}
	for row, d := range content {
		if ok, hLine, rLine := f(row, headers, d, sJoin(d, ",")); ok {
			headerLine = hLine
			allLines = append(allLines, rLine)
		}
	}

	contentLines := sJoin(allLines, "\n")
	if outcsv != "" {
		outcsv = sTrimSuffix(outcsv, ".csv") + ".csv"
		mustWriteFile(outcsv, []byte(headerLine+"\n"+contentLines))
	}

	return headerLine, contentLines, nil
}

// File2Rows :
func File2Rows(csvpath string, f func(i int, headers, items []string, line string) (ok bool, headerline, rowline string), outcsv string) (string, string, error) {
	csvFile, err := os.Open(csvpath)
	failOnErr("The file is not found || wrong root : %v", err)
	defer csvFile.Close()
	return ReaderByRow(csvFile, f, outcsv)
}

// SubFile :
func SubFile(csvpath string, incCol bool, columns []string, incRow bool, iRows []int, outcsv string) (string, string, error) {

	fnCol, fnRow := notexist, notexist
	if incCol {
		fnCol = exist
	}
	if incRow {
		fnRow = exist
	}

	return File2Rows(csvpath, func(i int, headers, items []string, line string) (bool, string, string) {

		// select needed columns
		gCols := cvt2GSlc(columns)
		cIdxGrp := []interface{}{}
		for i, header := range headers {
			switch {
			case fnCol(header, gCols...):
				cIdxGrp = append(cIdxGrp, i)
			}
		}

		// filter columns
		headersLeft := []string{}
		for i, header := range headers {
			if exist(i, cIdxGrp...) {
				if sContains(header, ",") {
					header = "\"" + header + "\""
				}
				headersLeft = append(headersLeft, header)
			}
		}
		headerLine := sJoin(headersLeft, ",")

		itemsLeft := []string{}
		for i, item := range items {
			if exist(i, cIdxGrp...) {
				if sContains(item, ",") {
					item = "\"" + item + "\""
				}
				itemsLeft = append(itemsLeft, item)
			}
		}
		itemLine := sJoin(itemsLeft, ",")

		// even if no rows to be returned, headerline still be returned
		if incRow && (iRows == nil || len(iRows) == 0) {
			return true, headerLine, ""
		}

		// select needed rows
		if fnRow(i, cvt2GSlc(iRows)...) {
			return true, headerLine, itemLine
		}

		return false, "", ""

	}, outcsv)
}

// func SelFile()
