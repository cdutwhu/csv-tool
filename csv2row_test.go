package csvtool

import (
	"os"
	"testing"
)

func TestCSV2ROW(t *testing.T) {
	enableLog2F(true, "./TestCSV2ROW.log")

	dir := "./data/"
	files, err := os.ReadDir(dir)
	failOnErr("%v", err)

	for _, file := range files {
		fName := dir + file.Name()
		if !sHasSuffix(file.Name(), ".csv") {
			continue
		}
		// if file.Name() != "data.csv" {
		// 	continue
		// }

		fPln(fName)
		// File2Rows(fName, func(i int, headers, items []string, line string) (bool, string, string) {

		// 	IdxRmGrp := []interface{}{}
		// 	for i, header := range headers {
		// 		switch header {
		// 		case "Item Response", "StartTime":
		// 			IdxRmGrp = append(IdxRmGrp, i)
		// 		}
		// 	}

		// 	headersLeft := []string{}
		// 	for i, header := range headers {
		// 		if !exist(i, IdxRmGrp...) {
		// 			if sContains(header, ",") {
		// 				header = "\"" + header + "\""
		// 			}
		// 			headersLeft = append(headersLeft, header)
		// 		}
		// 	}

		// 	itemsLeft := []string{}
		// 	for i, item := range items {
		// 		if !exist(i, IdxRmGrp...) {
		// 			if sContains(item, ",") {
		// 				item = "\"" + item + "\""
		// 			}
		// 			itemsLeft = append(itemsLeft, item)
		// 		}
		// 	}

		// 	if i < 5 {
		// 		return true, sJoin(headersLeft, ","), sJoin(itemsLeft, ",")
		// 	}

		// 	return false, "", ""

		// }, "out/"+file.Name())
	}
}
