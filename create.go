package csvtool

import (
	"github.com/digisan/go-generics/i64"
	"github.com/digisan/go-generics/i64s"
	"github.com/digisan/go-generics/si64"
	"github.com/digisan/go-generics/str"
)

// Create : create csv file with input headers
func Create(outcsv string, hdrNames ...string) (string, error) {
	if hdrNames == nil {
		return "", fEf("No Headers Provided")
	}

	headers := str.FM(hdrNames, nil, func(i int, e string) string { return mkValid(e) })
	hdrRow := sJoin(headers, ",")
	if outcsv != "" {
		mustWriteFile(outcsv, []byte(hdrRow))
	}
	return hdrRow, nil
}

// AppendRows : extend rows, append rows content to csv file
func AppendRows(csvpath string, validate bool, rows ...string) {
	if len(rows) > 0 {
		mustAppendFile(csvpath, []byte(sJoin(rows, "\n")), true)
	}
	if validate {
		File2Rows(csvpath, nil, true, "")
	}
}

// Combine : extend columns, linkHeaders combination must be UNIQUE in csvfileA & csvfileB
func Combine(csvfileA, csvfileB string, linkHeaders []string, onlyLinkedRow bool, outcsv string) {

	headersA, _, err := FileInfo(csvfileA)
	failOnErr("%v", err)
	failOnErrWhen(!str.SuperEq(headersA, linkHeaders), "%v", fEf("headers of csv-A must have every link header"))

	headersB, _, err := FileInfo(csvfileB)
	failOnErr("%v", err)
	failOnErrWhen(!str.SuperEq(headersB, linkHeaders), "%v", fEf("headers of csv-B must have every link header"))

	Create(outcsv, str.MkSet(str.Union(headersA, headersB)...)...)

	lkIndicesA := si64.FM(linkHeaders, nil, func(i int, e string) int { return str.IdxOf(e, headersA...) })
	lkIndicesB := si64.FM(linkHeaders, nil, func(i int, e string) int { return str.IdxOf(e, headersB...) })
	emptyComma := sRepeat(",", len(headersB)-len(linkHeaders))
	lkItemsGrp := [][]string{}
	mAiBr := make(map[int]string)

	_, rowsA, _ := File2Rows(
		csvfileA,
		func(i, n int, headers, items []string) (bool, string, string) {

			lkrItems := i64s.FM(lkIndicesA, nil, func(i, e int) string { return items[e] })
			lkItemsGrp = append(lkItemsGrp, lkrItems)
			items4w := str.FM(items, nil, func(i int, e string) string { return mkValid(e) })
			return true, "", sJoin(items4w, ",")
		},
		false,
		"",
	)

	File2Rows(
		csvfileB,
		func(i, n int, headers, items []string) (bool, string, string) {
			for iAtRowA, lkrItems := range lkItemsGrp {
				if str.Superset(items, lkrItems) {
					items4w := str.FM(items,
						func(i int, e string) bool { return i64.NotIn(i, lkIndicesB...) },
						func(i int, e string) string { return mkValid(e) },
					)
					mAiBr[iAtRowA] = sJoin(items4w, ",")
					return false, "", ""
				}
			}
			return false, "", ""
		},
		false,
		"",
	)

	rowsC := []string{}
	if onlyLinkedRow {
		for i, rA := range rowsA {
			if rB, ok := mAiBr[i]; ok {
				rowsC = append(rowsC, rA+","+rB)
			}
		}
	} else {
		for i, rA := range rowsA {
			if rB, ok := mAiBr[i]; ok {
				rowsC = append(rowsC, rA+","+rB)
			} else {
				rowsC = append(rowsC, rA+emptyComma)
			}
		}
	}

	AppendRows(outcsv, true, rowsC...)
}
