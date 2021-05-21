package csvtool

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
)

func TestCSV2ROW(t *testing.T) {
	enableLog2F(true, "./TestCSV2ROW.log")

	dir := "./data/"
	files, err := os.ReadDir(dir)
	failOnErr("%v", err)

	for _, file := range files {
		fName := filepath.Join(dir, file.Name())
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

func TestFileColumn(t *testing.T) {
	type args struct {
		csvpath string
		idx     int
	}
	tests := []struct {
		name      string
		args      args
		wantHdr   string
		wantItems []string
		wantErr   bool
	}{
		// TODO: Add test cases.
		{
			name: "OK",
			args: args{
				csvpath: "./data/data.csv",
				idx:     1,
			},
			wantHdr:   `"Name,Name1"`,
			wantItems: []string{`Ahmad,Ahmad`, "Hello", `Test1`, `Test2`, `[""abc]`},
			wantErr:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotHdr, gotItems, err := FileColumn(tt.args.csvpath, tt.args.idx)
			if (err != nil) != tt.wantErr {
				t.Errorf("FileColumn() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotHdr != tt.wantHdr {
				t.Errorf("FileColumn() gotHdr = %v, want %v", gotHdr, tt.wantHdr)
			}
			if !reflect.DeepEqual(gotItems, tt.wantItems) {
				t.Errorf("FileColumn() gotItems = %v, want %v", gotItems, tt.wantItems)
			}
		})
	}
}

func TestFileColAttr(t *testing.T) {
	type args struct {
		csvpath string
		idx     int
	}
	tests := []struct {
		name    string
		args    args
		want    *ColumnAttr
		wantErr bool
	}{
		// TODO: Add test cases.
		{
			name: "OK",
			args: args{
				csvpath: "./data/itemResults999.csv",
				idx:     10,
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "OK",
			args: args{
				csvpath: "./data/Substrands.csv",
				idx:     0,
			},
			want:    nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spew.Dump(FileColAttr(tt.args.csvpath, tt.args.idx))
		})
	}
}
