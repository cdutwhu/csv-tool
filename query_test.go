package csvtool

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/digisan/gotk/io"
)

func TestSubset(t *testing.T) {

	defer trackTime(time.Now())
	enableLog2F(true, "./TestSubset.log")

	dir := "./data/"
	files, err := os.ReadDir(dir)
	failOnErr("%v", err)

	for _, file := range files {
		fName := filepath.Join(dir, file.Name())
		if !sHasSuffix(file.Name(), ".csv") {
			continue
		}
		// if file.Name() != "itemResults1.csv" {
		// 	continue
		// }

		func() {

			fPln(fName)
			_, n, _ := FileInfo(fName)

			in, err := os.ReadFile(fName)
			failOnErr("%v", err)

			mustCreateDir("out/")
			file4w, err := os.OpenFile("out/"+file.Name(), os.O_WRONLY|os.O_CREATE, 0644)
			failOnErr("%v", err)
			defer file4w.Close()
			Subset(in, false, []string{"Domain", "Item Response", "YrLevel", "School", "Age", "substrand_id"}, true, iter2slc(n-1, -1), file4w)

			mustCreateDir("out1/")
			file4w1, err := os.OpenFile("out1/"+file.Name(), os.O_WRONLY|os.O_CREATE, 0644)
			failOnErr("%v", err)
			defer file4w1.Close()
			Subset(in, true, []string{"School", "Domain", "YrLevel", "XXX", "Test Name", "Test level", "Test Domain", "Test Item RefID", "Item Response"}, true, iter2slc(0, 20000), file4w1)

		}()
	}
}

func TestSelect(t *testing.T) {

	defer trackTime(time.Now())
	enableLog2F(true, "./TestSelect.log")

	dir := "./data/"
	files, err := os.ReadDir(dir)
	failOnErr("%v", err)

	for _, file := range files {
		fName := filepath.Join(dir, file.Name())
		if !sHasSuffix(fName, ".csv") {
			continue
		}

		fPln(fName)

		func() {

			in, err := os.ReadFile(fName)
			failOnErr("%v", err)

			mustWriteFile("out/"+file.Name(), []byte{})
			file4w, err := os.OpenFile("out/"+file.Name(), os.O_WRONLY|os.O_CREATE, 0666)
			failOnErr("%v", err)
			defer file4w.Close()

			Select(in, '&', []Condition{
				{Hdr: "School", Val: "21221", ValTyp: "string", Rel: "="},
				{Hdr: "Domain", Val: "Spelling", ValTyp: "string", Rel: "="},
				{Hdr: "YrLevel", Val: 3, ValTyp: "int", Rel: "<="},
			}, file4w)

		}()
	}
}

func TestQuery(t *testing.T) {

	defer trackTime(time.Now())
	enableLog2F(true, "./TestQuery.log")

	dir := "./data"
	files, err := os.ReadDir(dir)
	failOnErr("%v", err)

	n := len(files)
	fPln(n, "files")

	wg := &sync.WaitGroup{}
	wg.Add(n)

	for _, file := range files {

		go func(filename string) {
			defer wg.Done()

			if !sHasSuffix(filename, ".csv") {
				return
			}

			fName := filepath.Join(dir, filename)
			fPln(fName)

			QueryFile(
				fName,
				true,
				[]string{
					"Domain",
					"School",
					"YrLevel",
					"Test Name",
					"Test level",
					"Test Domain",
					"Test Item RefID",
				},
				'&',
				[]Condition{
					{Hdr: "School", Val: "21221", ValTyp: "string", Rel: "="},
					{Hdr: "YrLevel", Val: 5, ValTyp: "uint", Rel: ">"},
					{Hdr: "Domain", Val: "Reading", ValTyp: "string", Rel: "!="},
				},
				"out/"+filename,
			)

		}(file.Name())
	}

	wg.Wait()

	fmt.Println(io.FileDirCount(dir, true))
	fmt.Println(io.FileDirCount("out/", true))
}

func TestQueryAtConfig(t *testing.T) {
	n, err := QueryAtConfig("./queryconfig/query.toml")
	failOnErr("%v", err)
	fPln(n)
}

func TestUnique(t *testing.T) {
	type args struct {
		csvpath string
		outcsv  string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		want1   []string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := Unique(tt.args.csvpath, tt.args.outcsv)
			if (err != nil) != tt.wantErr {
				t.Errorf("Unique() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Unique() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("Unique() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
