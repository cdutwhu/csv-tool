package csvtool

import (
	"os"
	"testing"
)

func TestCSV2ROW(t *testing.T) {
	enableLog2F(true, "./err.log")

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

func TestSubFile(t *testing.T) {
	enableLog2F(true, "./err.log")

	dir := "./data/"
	files, err := os.ReadDir(dir)
	failOnErr("%v", err)

	for _, file := range files {
		fName := dir + file.Name()
		if !sHasSuffix(file.Name(), ".csv") {
			continue
		}
		// if file.Name() != "compareItemWriting.csv" {
		// 	continue
		// }

		fPln(fName)
		// SubFile(fName, false, []string{"Item Response", "YrLevel", "School", "Age"}, true, []int{0, 1}, "out/"+file.Name())
		SubFile(fName, true, []string{"Item Response", "YrLevel", "School", "Age", "Id"}, true, iter2Slc(0, 4), "out/"+file.Name())
	}
}
