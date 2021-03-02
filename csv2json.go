package csvtool

import (
	"encoding/csv"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// File2JSON : read the content of CSV File
func File2JSON(path string, vertical, save bool, savePaths ...string) (string, []string) {
	csvFile, err := os.Open(path)
	failOnErr("The file is not found || wrong root : %v", err)
	defer csvFile.Close()
	jsonstr, headers, err := Reader2JSON(csvFile, path)
	failOnErr("%v", err)

	if vertical {
		jsonstr = jsonScalarSelX(jsonstr, headers...)
	}

	if save {
		if len(savePaths) == 0 {
			newFileName := filepath.Base(path)
			newFileName = newFileName[0:len(newFileName)-len(filepath.Ext(newFileName))] + ".json"
			savepath := filepath.Join(filepath.Dir(path), newFileName)
			mustWriteFile(savepath, []byte(jsonstr))
		}
		for _, savepath := range savePaths {
			mustWriteFile(savepath, []byte(jsonstr))
		}
	}
	return jsonstr, headers
}

// Reader2JSON to
func Reader2JSON(r io.Reader, description string) (string, []string, error) {
	content, _ := csv.NewReader(r).ReadAll()
	if len(content) < 1 {
		return "", nil, fEf("FILE_EMPTY")
	}

	headers := make([]string, 0)
	for i, headE := range content[0] {
		if headE == "" {
			headE = fSf("column_%d", i)
			fPln(warnOnErr("%v: %s - column[%d] is empty, mark [%s]", fEf("CSV_COLUMN_HEADER_EMPTY"), description, i, headE))
		}
		headers = append(headers, headE)
	}

	// Remove the header row
	content = content[1:]

	// Set Column Type
	mColType := make(map[int]rune)
	for _, d := range content {
		for col, y := range d {
			_, fErr := strconv.ParseFloat(y, 32)
			_, bErr := strconv.ParseBool(y)
			switch {
			case fErr == nil && mColType[col] != 'S':
				mColType[col] = 'N'
			case bErr == nil && mColType[col] != 'S':
				mColType[col] = 'B'
			default:
				mColType[col] = 'S'
			}
		}
	}
	//

	// var buffer bytes.Buffer
	var sb strings.Builder
	sb.WriteString("[")
	for row, d := range content {
		sb.WriteString("{")

		for col, y := range d {
			sb.WriteString(`"` + headers[col] + `":`)

			// _, fErr := strconv.ParseFloat(y, 32)
			// _, bErr := strconv.ParseBool(y)
			// if fErr == nil {
			// 	sb.WriteString(y)
			// } else if bErr == nil {
			// 	sb.WriteString(strings.ToLower(y))
			// } else {
			// 	sb.WriteString((`"` + y + `"`))
			// }

			switch mColType[col] {
			case 'N':
				if sHasPrefix(y, ".") {
					y = "0" + y
				}
				sb.WriteString(y)
			case 'B':
				sb.WriteString(strings.ToLower(y))
			case 'S':
				y = sReplaceAll(y, `"`, `\"`)
				y = sReplaceAll(y, "\n", "\\n")
				sb.WriteString(`"` + y + `"`)

				// deal with array value
				// if len(y) > 0 && y[0] == '[' && y[len(y)-1] == ']' {
				// 	arrcont := y[1 : len(y)-1]
				// 	elements := []string{}
				// 	for _, ele := range sSplit(arrcont, ",") {
				// 		elements = append(elements, fSf("\"%s\"", ele))
				// 	}
				// 	sb.WriteString(`[` + sJoin(elements, ",") + `]`)
				// } else {
				// 	sb.WriteString(`"` + y + `"`)
				// }
			}

			//end of property
			if col < len(d)-1 {
				sb.WriteString(",")
			}
		}
		//end of object of the array
		sb.WriteString("}")
		if row < len(content)-1 {
			sb.WriteString(",")
		}
	}

	sb.WriteString(`]`)
	rawMessage := json.RawMessage(sb.String())
	jsonstr := string(rawMessage)

	// if !isValidJSON(jsonstr) {
	// 	os.WriteFile("./err.json", []byte(jsonstr), 0666)
	// }

	failOnErrWhen(!isValidJSON(jsonstr), "%v", fEf("Invalid JSON string")) // validate output json
	// return jsonstr, headers, nil
	jsonbytes, err := json.MarshalIndent(rawMessage, "", "  ")
	failOnErr("%v", err)
	return string(jsonbytes), headers, nil
}
