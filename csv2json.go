package csvtool

import (
	"encoding/csv"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/cdutwhu/n3-util/n3err"
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
		return "", nil, n3err.FILE_EMPTY
	}

	headers := make([]string, 0)
	for i, headE := range content[0] {
		if headE == "" {
			headE = fSf("column_%d", i)
			fPln(warnOnErr("%v: %s - column[%d] is empty, mark [%s]", n3err.CSV_COLUMN_HEADER_EMPTY, description, i, headE))
		}
		headers = append(headers, headE)
	}

	//Remove the header row
	content = content[1:]

	// Set Column Type
	mColType := make(map[int]rune)
	for _, d := range content {
		for j, y := range d {
			_, fErr := strconv.ParseFloat(y, 32)
			_, bErr := strconv.ParseBool(y)
			switch {
			case fErr == nil:
				mColType[j] = 'N'
			case bErr == nil:
				mColType[j] = 'B'
			default:
				mColType[j] = 'S'
			}
		}
	}
	//

	// var buffer bytes.Buffer
	var buffer strings.Builder
	buffer.WriteString("[")
	for i, d := range content {
		buffer.WriteString("{")
		for j, y := range d {
			buffer.WriteString(`"` + headers[j] + `":`)

			// _, fErr := strconv.ParseFloat(y, 32)
			// _, bErr := strconv.ParseBool(y)
			// if fErr == nil {
			// 	buffer.WriteString(y)
			// } else if bErr == nil {
			// 	buffer.WriteString(strings.ToLower(y))
			// } else {
			// 	buffer.WriteString((`"` + y + `"`))
			// }

			switch mColType[j] {
			case 'N':
				buffer.WriteString(y)
			case 'B':
				buffer.WriteString(strings.ToLower(y))
			case 'S':
				buffer.WriteString((`"` + y + `"`))
			}

			//end of property
			if j < len(d)-1 {
				buffer.WriteString(",")
			}
		}
		//end of object of the array
		buffer.WriteString("}")
		if i < len(content)-1 {
			buffer.WriteString(",")
		}
	}

	buffer.WriteString(`]`)
	rawMessage := json.RawMessage(buffer.String())
	jsonstr, err := json.MarshalIndent(rawMessage, "", "  ")
	failOnErr("%v", err)
	return string(jsonstr), headers, nil
}