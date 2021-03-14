package csvtool

import (
	"os"
	"testing"
	"time"

	"github.com/digisan/gotk"
)

func TestCSV2JSON(t *testing.T) {
	defer gotk.TrackTime(time.Now())
	enableLog2F(true, "./TestCSV2JSON.log")

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
		File2JSON(fName, false, true, sReplaceAll(fName, ".csv", ".json"))
		File2JSON(fName, true, true, sReplaceAll(fName, ".csv", "1.json"))
	}

	// path := flag.String("path", "./data/ModulePrerequisites.csv", "Path of the file")
	// flag.Parse()
	// File2JSON(*path, true, "data.json")
	// fmt.Println(sRepeat("=", 10), "Done", sRepeat("=", 10))
}

func BenchmarkCSV2JSON(b *testing.B) {
	path := "../data/csv/data.csv"
	for n := 0; n < b.N; n++ {
		csv, _ := os.Open(path)
		Reader2JSON(csv, path)
		csv.Close()
	}
}
