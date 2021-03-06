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
func ReaderByRow(r io.Reader, f func(i, n int, headers []string, items []interface{}) (ok bool, hdrRow, row string), outcsv string) (string, []string, error) {
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
		if ok, hRow, row := f(i, N, headers, toGSlc(d)); ok {
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
func File2Rows(csvpath string, f func(i, n int, headers []string, items []interface{}) (ok bool, hdrRow, row string), outcsv string) (string, string, error) {
	csvFile, err := os.Open(csvpath)
	failP1OnErr("The file is not found || wrong root : %v", err)
	defer csvFile.Close()
	hRow, rows, err := ReaderByRow(csvFile, f, outcsv)
	return hRow, sJoin(rows, "\n"), err
}
