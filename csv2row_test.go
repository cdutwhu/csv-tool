package csvtool

import (
	"os"
	"testing"
	"time"
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

func TestSubset(t *testing.T) {
	defer trackTime(time.Now())
	enableLog2F(true, "./TestSubset.log")

	dir := "./data1/"
	files, err := os.ReadDir(dir)
	failOnErr("%v", err)

	for _, file := range files {
		fName := dir + file.Name()
		if !sHasSuffix(file.Name(), ".csv") {
			continue
		}
		// if file.Name() != "itemResults1.csv" {
		// 	continue
		// }

		fPln(fName)
		_, n, _ := FileInfo(fName)
		Subset(fName, false, []string{"Item Response", "YrLevel", "School", "Age", "substrand_id"}, true, iter2slc(n-1, -1), "out/"+file.Name())
		Subset(fName, true, []string{"School", "YrLevel", "Domain", "Test Name", "Test level", "Test Domain", "Test Item RefID", "Item Response"}, true, iter2slc(0, 20000), "out/"+file.Name())
	}
}

func TestSelect(t *testing.T) {
	defer trackTime(time.Now())
	enableLog2F(true, "./TestSelect.log")

	dir := "./data1/"
	files, err := os.ReadDir(dir)
	failOnErr("%v", err)

	for _, file := range files {
		fName := dir + file.Name()
		if !sHasSuffix(file.Name(), ".csv") {
			continue
		}
		// if file.Name() != "itemResults1.csv" {
		// 	continue
		// }

		fPln(fName)
		Select(fName, '&', []struct {
			header   string
			value    interface{}
			valtype  string
			relation string
		}{
			// {header: "School", value: "21221", valtype: "string", relation: "!="},
			{header: "Domain", value: "Reading", valtype: "string", relation: "="},
			{header: "Response Correctness", value: "Correct", valtype: "string", relation: "="},
			{header: "Item Response", value: "", valtype: "string", relation: "="},
		}, "out/"+file.Name())
	}
}
