package csvtool

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"github.com/BurntSushi/toml"
	"github.com/cdutwhu/csv-tool/queryconfig"
	"github.com/digisan/gotk/generics/ti"
	"github.com/digisan/gotk/generics/ti32"
	"github.com/digisan/gotk/generics/ts"
	"github.com/digisan/gotk/generics/tsi"
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
func Subset(in []byte, incColMode bool, hdrNames []string, incRowMode bool, iRows []int, w io.Writer) (string, []string, error) {

	fnRow := ti.NotIn
	if incRowMode {
		fnRow = ti.In
	}

	cIndices, hdrRow := []int{}, ""
	fast, min, max := isContInts(iRows)

	return ScanByRow(in, func(idx, cnt int, headers, items []string) (bool, string, string) {

		// get [hdrRow], [cIndices] once
		if hdrRow == "" {
			// select needed columns, cIndices is qualified header's original index in file headers
			var hdrRt []string
			if incColMode {
				cIndices = tsi.FM(hdrNames,
					func(i int, e string) bool { return ts.In(e, headers...) },
					func(i int, e string) int { return ts.IdxOf(e, headers...) },
				)
				hdrRt = ts.Reorder(headers, cIndices) // Reorder has filter
				hdrRt = ts.FM(hdrRt, nil, func(i int, e string) string { return mkValid(e) })
			} else {
				cIndices = tsi.FM(headers,
					func(i int, e string) bool { return ts.NotIn(e, hdrNames...) },
					func(i int, e string) int { return i },
				)
				hdrRt = ts.FM(headers,
					func(i int, e string) bool { return ti.In(i, cIndices...) },
					func(i int, e string) string { return mkValid(e) },
				)
			}
			hdrRow = sJoin(hdrRt, ",")
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
			var itemsRt []string
			if incColMode {
				itemsRt = ts.Reorder(items, cIndices)
				itemsRt = ts.FM(itemsRt, nil, func(i int, e string) string { return mkValid(e) })
			} else {
				itemsRt = ts.FM(items,
					func(i int, e string) bool { return ti.In(i, cIndices...) },
					func(i int, e string) string { return mkValid(e) },
				)
			}

			return true, hdrRow, sJoin(itemsRt, ",")
		}

		return true, hdrRow, "" // still "ok" as hdrRow is needed even if empty content

	}, !incColMode, w)
}

// Condition :
type Condition struct {
	Hdr    string
	Val    interface{}
	ValTyp string
	Rel    string
}

// Select : R : [&, |]; condition relation : [=, !=, >, <, >=, <=]
// [=, !=] only apply to string comparasion, [>, <, >=, <=] apply to number comparasion
func Select(in []byte, R rune, CGrp []Condition, w io.Writer) (string, []string, error) {

	failP1OnErrWhen(ti32.NotIn(R, '&', '|'), "%v", fEf("R can only be [&, |]"))
	nCGrp := len(CGrp)

	return ScanByRow(in, func(idx, cnt int, headers, items []string) (bool, string, string) {

		hdrNames := ts.FM(headers, nil, func(i int, e string) string { return mkValid(e) })
		hdrRow := sJoin(hdrNames, ",")

		if len(items) == 0 {
			return true, hdrRow, ""
		}

		CResults := []interface{}{}

	NEXTCONDITION:
		for _, C := range CGrp {

			if R == '|' && len(CResults) > 0 {
				break NEXTCONDITION
			}

			if I := ts.IdxOf(C.Hdr, headers...); I != -1 {
				iVal := items[I]

				if C.Rel == "=" {
					if iVal == C.Val {
						CResults = append(CResults, struct{}{})
					}
					continue NEXTCONDITION
				}
				if C.Rel == "!=" {
					if iVal != C.Val {
						CResults = append(CResults, struct{}{})
					}
					continue NEXTCONDITION
				}

				switch C.ValTyp {
				case "int", "int8", "int16", "int32", "int64":
					var cValue int64
					if i64Val, ok := C.Val.(int64); ok {
						cValue = i64Val
					} else if intVal, ok := C.Val.(int); ok {
						cValue = int64(intVal)
					}

					iValue, err := strconv.ParseInt(iVal, 10, 64)
					if err != nil {
						break
					}
					if (C.Rel == ">" && iValue > cValue) ||
						(C.Rel == ">=" && iValue >= cValue) ||
						(C.Rel == "<" && iValue < cValue) ||
						(C.Rel == "<=" && iValue <= cValue) {
						CResults = append(CResults, struct{}{})
					}

				case "uint", "uint8", "uint16", "uint32", "uint64":
					var cValue uint64
					if i64Val, ok := C.Val.(int64); ok {
						cValue = uint64(i64Val)
					} else if intVal, ok := C.Val.(int); ok {
						cValue = uint64(intVal)
					}

					iValue, err := strconv.ParseUint(iVal, 10, 64)
					if err != nil {
						break
					}
					if (C.Rel == ">" && iValue > cValue) ||
						(C.Rel == ">=" && iValue >= cValue) ||
						(C.Rel == "<" && iValue < cValue) ||
						(C.Rel == "<=" && iValue <= cValue) {
						CResults = append(CResults, struct{}{})
					}

				case "float32", "float64", "float", "double":
					cValue := C.Val.(float64)
					iValue, err := strconv.ParseFloat(iVal, 64)
					if err != nil {
						break
					}
					if (C.Rel == ">" && iValue > cValue) ||
						(C.Rel == ">=" && iValue >= cValue) ||
						(C.Rel == "<" && iValue < cValue) ||
						(C.Rel == "<=" && iValue <= cValue) {
						CResults = append(CResults, struct{}{})
					}

				default:
					panic("comparable type [" + C.ValTyp + "] is not supported")
				}
			}
		}

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

	}, true, w)
}

// Query : combine Subset(incColMode, all rows) & Select
func Query(in []byte, incColMode bool, hdrNames []string, R rune, CGrp []Condition, w io.Writer) (string, []string, error) {

	b := &bytes.Buffer{}
	_, _, err := Select(in, R, CGrp, io.Writer(b))
	if err == nil {
		return Subset(b.Bytes(), incColMode, hdrNames, false, []int{}, w)
	}
	return "", nil, err

}

func QueryFile(csvpath string, incColMode bool, hdrNames []string, R rune, CGrp []Condition, outcsv string) error {

	// fPf("---querying...<%s>\n", csvpath)

	if !fileExists(csvpath) {
		return fEf("[%s] does NOT exist, ignore", csvpath)
	}

	if csvpath == outcsv {
		outcsv += ".csv"
		defer func() {
			os.Remove(csvpath)
			os.Rename(outcsv, csvpath)
		}()
	}

	in, err := os.ReadFile(csvpath)
	failP1OnErr("%v", err)

	mustCreateDir(filepath.Dir(outcsv))

	fw, err := os.OpenFile(outcsv, os.O_WRONLY|os.O_CREATE, 0666)
	failP1OnErr("%v", err)
	defer fw.Close()

	_, _, err = Query(in, incColMode, hdrNames, R, CGrp, fw)
	return err

}

// QueryAtConfig :
func QueryAtConfig(tomlPath string) (int, error) {

	config := &queryconfig.QueryConfig{}
	if _, err := toml.DecodeFile(tomlPath, config); err != nil {
		return 0, err
	}
	// failOnErr("%v", err)

	for _, qry := range config.Query {

		cond := []Condition{}

		for _, c := range qry.Cond {
			cond = append(cond, Condition{Hdr: c.Header, Val: c.Value, ValTyp: c.ValueType, Rel: c.RelaOfItemValue})
		}

		fPln("Processing ... " + qry.Name)

		QueryFile(
			qry.CsvPath,
			qry.IncColMode,
			qry.HdrNames,
			rune(qry.RelaOfCond[0]),
			cond,
			qry.OutCsvPath,
		)
	}

	return len(config.Query), nil
}
