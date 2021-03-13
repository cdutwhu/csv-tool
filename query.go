package csvtool

import (
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/cdutwhu/csv-tool/queryconfig"
	"github.com/digisan/gotk/slice/ti"
	"github.com/digisan/gotk/slice/ti32"
	"github.com/digisan/gotk/slice/ts"
	"github.com/digisan/gotk/slice/tsi"
	"github.com/google/uuid"
)

// Unique : remove repeated items
func Unique(csvpath, outcsv string) (string, []string, error) {
	defer File2Rows(outcsv, nil, true, "")

	m := make(map[string]struct{})
	return File2Rows(
		csvpath,
		func(idx, cnt int, headers, items []string) (bool, string, string) {
			row := sJoin(items, ",")
			if _, ok := m[row]; ok {
				return false, "", ""
			}
			m[row] = struct{}{}

			headers4w := ts.FM(headers, nil, func(i int, e string) string { return mkValid(e) })
			items4w := ts.FM(items, nil, func(i int, e string) string { return mkValid(e) })
			return true, sJoin(headers4w, ","), sJoin(items4w, ",")
		},
		true,
		outcsv,
	)
}

// Subset : content iRow start from 0. i.e. 1st content row index is 0
func Subset(csvpath string, incColMode bool, hdrNames []string, incRowMode bool, iRows []int, outcsv string) (string, []string, error) {

	fnCol, fnRow := ts.NotIn, ti.NotIn
	if incColMode {
		fnCol = ts.In
	}
	if incRowMode {
		fnRow = ti.In
	}

	cIndices, hdrRow := []int{}, ""
	fast, min, max := isContInts(iRows)

	return File2Rows(csvpath, func(idx, cnt int, headers, items []string) (bool, string, string) {

		// get [hdrRow], [cIndices] once
		if hdrRow == "" {
			// select needed columns
			cIndices = tsi.FM(headers,
				func(i int, e string) bool { return fnCol(e, hdrNames...) },
				func(i int, e string) int { return i },
			)
			// filter columns headers
			hdrLeft := ts.FM(headers,
				func(i int, e string) bool { return ti.In(i, cIndices...) },
				func(i int, e string) string { return mkValid(e) },
			)
			hdrRow = sJoin(hdrLeft, ",")
		}

		ok := false
		if fast {
			if (incRowMode && idx >= min && idx <= max) || (!incRowMode && (idx < min || idx > max)) {
				ok = true
			}
		} else {
			if fnRow(idx, iRows...) {
				ok = true
			}
		}

		if ok {
			// filter column items
			itemLeft := ts.FM(items,
				func(i int, e string) bool { return ti.In(i, cIndices...) },
				func(i int, e string) string { return mkValid(e) },
			)

			return true, hdrRow, sJoin(itemLeft, ",")
		}

		return true, hdrRow, "" // still "ok" as hdrRow is needed even if empty content

	}, !incColMode, outcsv)
}

// Select : R : [&, |]; condition relation : [=, !=, >, <, >=, <=]
func Select(csvpath string, R rune, CGrp []struct {
	header   string
	value    interface{}
	valtype  string
	relation string
}, outcsv string) (string, []string, error) {

	failP1OnErrWhen(ti32.NotIn(R, '&', '|'), "%v", fEf("R can only be [&, |]"))
	nCGrp := len(CGrp)

	return File2Rows(csvpath, func(idx, cnt int, headers, items []string) (bool, string, string) {
		CResults := []interface{}{}

	NEXTCONDITION:
		for _, C := range CGrp {

			if R == '|' && len(CResults) > 0 {
				break NEXTCONDITION
			}

			if I := ts.IdxOf(C.header, headers...); I != -1 {
				iVal := items[I]
				cVal, cValType, cR := C.value, C.valtype, C.relation

				if cR == "=" {
					if iVal == cVal {
						CResults = append(CResults, struct{}{})
					}
					continue NEXTCONDITION
				}
				if cR == "!=" {
					if iVal != cVal {
						CResults = append(CResults, struct{}{})
					}
					continue NEXTCONDITION
				}

				switch cValType {
				case "int", "int8", "int16", "int32", "int64":
					var cValue int64
					if i64Val, ok := cVal.(int64); ok {
						cValue = i64Val
					} else if intVal, ok := cVal.(int); ok {
						cValue = int64(intVal)
					}

					iValue, err := strconv.ParseInt(iVal, 10, 64)
					failOnErr("%v", err)
					if (cR == ">" && iValue > cValue) || (cR == ">=" && iValue >= cValue) || (cR == "<" && iValue < cValue) || (cR == "<=" && iValue <= cValue) {
						CResults = append(CResults, struct{}{})
					}

				case "uint", "uint8", "uint16", "uint32", "uint64":
					var cValue uint64
					if i64Val, ok := cVal.(int64); ok {
						cValue = uint64(i64Val)
					} else if intVal, ok := cVal.(int); ok {
						cValue = uint64(intVal)
					}

					iValue, err := strconv.ParseUint(iVal, 10, 64)
					failOnErr("%v", err)
					if (cR == ">" && iValue > cValue) || (cR == ">=" && iValue >= cValue) || (cR == "<" && iValue < cValue) || (cR == "<=" && iValue <= cValue) {
						CResults = append(CResults, struct{}{})
					}

				case "float32", "float64", "float", "double":
					cValue := cVal.(float64)
					iValue, err := strconv.ParseFloat(iVal, 64)
					failOnErr("%v", err)
					if (cR == ">" && iValue > cValue) || (cR == ">=" && iValue >= cValue) || (cR == "<" && iValue < cValue) || (cR == "<=" && iValue <= cValue) {
						CResults = append(CResults, struct{}{})
					}

				default:
					panic("comparable type [" + cValType + "] is not supported")
				}
			}
		}

		hdrNames := ts.FM(headers, nil, func(i int, e string) string { return mkValid(e) })
		hdrRow := sJoin(hdrNames, ",")

		ok := false

		// Has conditions
		if len(CGrp) > 0 {
			if len(CResults) == 0 {
				return true, hdrRow, ""
			}
			if (R == '&' && len(CResults) == nCGrp) || (R == '|' && len(CResults) > 0) {
				ok = true
			}
		}

		// No conditions OR condition ok
		if ok || len(CGrp) == 0 {
			itemValues := ts.FM(items, nil, func(i int, e string) string { return mkValid(e) })
			return true, hdrRow, sJoin(itemValues, ",")
		}

		return true, hdrRow, ""

	}, true, outcsv)
}

// Query : combine Subset(incColMode, all rows) & Select
func Query(csvpath string, incColMode bool, hdrNames []string, R rune, CGrp []struct {
	header   string
	value    interface{}
	valtype  string
	relation string
}, outcsv string, wg *sync.WaitGroup) (string, []string, error) {

	filename := sTrimSuffix(filepath.Base(csvpath), ".csv")
	tempcsv := "./tempcsv/" + filename + "@" + uuid.NewString() + ".csv"
	defer func() {
		os.Remove(tempcsv)
		if wg != nil {
			wg.Done()
		}
	}()

	_, _, err := Select(csvpath, R, CGrp, tempcsv)
	time.Sleep(5 * time.Millisecond)
	if err == nil {
		return Subset(tempcsv, incColMode, hdrNames, false, []int{}, outcsv)
	}
	return "", nil, err
}

// QueryAtConfig :
func QueryAtConfig(tomlPath string) (int, error) {

	config := &queryconfig.QueryConfig{}
	if _, err := toml.DecodeFile(tomlPath, config); err != nil {
		return 0, err
	}
	// failOnErr("%v", err)

	wg := &sync.WaitGroup{}
	wg.Add(len(config.Query))

	for _, qry := range config.Query {

		cond := []struct {
			header   string
			value    interface{}
			valtype  string
			relation string
		}{}

		for _, c := range qry.Cond {
			cond = append(cond, struct {
				header   string
				value    interface{}
				valtype  string
				relation string
			}{header: c.Header, value: c.Value, valtype: c.ValueType, relation: c.RelaOfItemValue})
		}

		fPln("Processing ... " + qry.Name)

		go Query(qry.CsvPath,
			qry.IncColMode,
			qry.HdrNames,
			rune(qry.RelaOfCond[0]),
			cond,
			qry.OutCsvPath,
			wg,
		)
	}

	wg.Wait()

	return len(config.Query), nil
}
