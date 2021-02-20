package csvtool

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

func TestCSV2JSON(t *testing.T) {
	enableLog2F(true, "./err.log")

	dir := "./data/"
	files, err := ioutil.ReadDir(dir)
	failOnErr("%v", err)

	for _, file := range files {
		fName := dir + file.Name()
		fPln(fName)
		if !strings.HasSuffix(file.Name(), ".csv") {
			continue
		}
		File2JSON(fName, false, true, sReplaceAll(fName, ".csv", ".json"))
		File2JSON(fName, true, true, sReplaceAll(fName, ".csv", "1.json"))
	}

	// path := flag.String("path", "./data/ModulePrerequisites.csv", "Path of the file")
	// flag.Parse()
	// File2JSON(*path, true, "data.json")
	// fmt.Println(strings.Repeat("=", 10), "Done", strings.Repeat("=", 10))
}

func BenchmarkCSV2JSON(b *testing.B) {
	path := "../data/csv/data.csv"
	for n := 0; n < b.N; n++ {
		csv, _ := os.Open(path)
		Reader2JSON(csv, path)
		csv.Close()
	}
}