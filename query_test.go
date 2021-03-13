package csvtool

import (
	"os"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestSubset(t *testing.T) {

	defer trackTime(time.Now())
	enableLog2F(true, "./TestSubset.log")

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
		_, n, _ := FileInfo(fName)
		Subset(fName, false, []string{"Domain", "Item Response", "YrLevel", "School", "Age", "substrand_id"}, true, iter2slc(n-1, -1), "out/"+file.Name())
		Subset(fName, true, []string{"School", "Domain", "YrLevel", "XXX", "Test Name", "Test level", "Test Domain", "Test Item RefID", "Item Response"}, true, iter2slc(0, 20000), "out1/"+file.Name())
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

	wg := &sync.WaitGroup{}
	wg.Add(len(files))
	// wg.Add(1)

	for _, file := range files {
		fName := dir + file.Name()
		if !sHasSuffix(file.Name(), ".csv") {
			continue
		}
		// if file.Name() != "data.csv" {
		// 	continue
		// }

		fPln(fName)
		go Query(fName,
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
			[]struct {
				header   string
				value    interface{}
				valtype  string
				relation string
			}{
				{header: "School", value: "21221", valtype: "string", relation: "="},
				{header: "YrLevel", value: 5, valtype: "uint", relation: ">"},
				{header: "Domain", value: "Reading", valtype: "string", relation: "!="},
			},
			"out/"+file.Name(),
			wg)
	}

	wg.Wait()
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
