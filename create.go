package csvtool

func fortest() {
	headersC := toSet(union([]string{"a", "b", "c"}, []string{"a", "c", "d"})).([]string)
	fPln(headersC)
}

// Create : create csv file with input headers
func Create(outcsv string, hdrNames ...string) (string, error) {
	if len(hdrNames) == 0 {
		return "", fEf("No Headers Provided")
	}
	headers := []string{}
	for _, hdr := range hdrNames {
		headers = append(headers, mkValid(hdr))
	}
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
	return
}

// Combine : extend columns, linkHeaders combination must be UNIQUE in csvfileA & csvfileB
func Combine(csvfileA, csvfileB string, linkHeaders []string, onlyKeepLinkRow bool, outcsv string) {

	headersA, _, err := FileInfo(csvfileA)
	failOnErr("%v", err)
	ok, _ := cover(headersA, linkHeaders)
	failOnErrWhen(!ok, "%v", fEf("headers of csv-A must have every link header"))

	headersB, _, err := FileInfo(csvfileB)
	failOnErr("%v", err)
	ok, _ = cover(headersB, linkHeaders)
	failOnErrWhen(!ok, "%v", fEf("headers of csv-B must have every link header"))

	gHeadersA, gHeadersB := toGSlc(headersA), toGSlc(headersB)

	Create(outcsv, toSet(union(headersA, headersB)).([]string)...)

	lkIndicesA, lkIndicesB := []int{}, []int{}
	for _, lkHdr := range linkHeaders {
		lkIndicesA = append(lkIndicesA, idxOf(lkHdr, gHeadersA...))
		lkIndicesB = append(lkIndicesB, idxOf(lkHdr, gHeadersB...))
	}

	gLkIndicesB := toGSlc(lkIndicesB)
	emptyComma := sRepeat(",", len(headersB)-len(linkHeaders))

	type ritems []interface{}
	lkItemsGrp := []ritems{}
	mAiBr := make(map[int]string)

	_, rowsA, _ := File2Rows(
		csvfileA,
		func(i, n int, headers []string, items []interface{}) (bool, string, string) {
			lkrItems := ritems{}
			for _, iLK := range lkIndicesA {
				lkrItems = append(lkrItems, items[iLK])
			}
			lkItemsGrp = append(lkItemsGrp, lkrItems)

			items4w := []string{}
			for _, item := range items {
				items4w = append(items4w, mkValid(item.(string)))
			}
			return true, "", sJoin(items4w, ",")
		},
		false,
		"",
	)

	File2Rows(
		csvfileB,
		func(i, n int, headers []string, items []interface{}) (bool, string, string) {
			for iAtRowA, lkrItems := range lkItemsGrp {
				if ok, _ := cover(items, lkrItems); ok {
					items4w := []string{}
					for iItem, item := range items {
						if notexist(iItem, gLkIndicesB...) {
							items4w = append(items4w, mkValid(item.(string)))
						}
					}
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
	if onlyKeepLinkRow {
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
