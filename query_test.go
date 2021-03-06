package csvtool

import (
	"os"
	"testing"
	"time"
)

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

	dir := "./data/"
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
			{header: "School", value: "21221", valtype: "string", relation: "="},
			{header: "Domain", value: "Reading", valtype: "string", relation: "="},
		}, "out/"+file.Name())
	}
}

func TestQuery(t *testing.T) {
	
	defer trackTime(time.Now())
	enableLog2F(true, "./TestQuery.log")

	dir := "./data/"
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
		Query(fName,
			[]string{
				"School",
				"YrLevel",
				"Domain",
				"Test Name",
				"Test level",
				"Test Domain",
				"Test Item RefID",
				"Response Correctness",
			},
			'&',
			[]struct {
				header   string
				value    interface{}
				valtype  string
				relation string
			}{
				{header: "School", value: "21221", valtype: "string", relation: "="},
				{header: "YrLevel", value: 5, valtype: "int", relation: ">"},
				{header: "Domain", value: "Reading", valtype: "string", relation: "="},
			},
			"out/"+file.Name())
	}
}
