package csvtool

import (
	"fmt"
	"testing"

	"github.com/digisan/gotk/io"
)

func TestSplit(t *testing.T) {
	type args struct {
		csvfile    string
		outdir     string
		categories []string
	}
	tests := []struct {
		name    string
		args    args
		want    []string
		wantErr bool
	}{
		// TODO: Add test cases.
		{
			name: "OK",
			args: args{
				csvfile:    "./data/qldStudent.csv",
				outdir:     "out",
				categories: []string{"School", "YrLevel", "Domain"},
			},
			want:    []string{},
			wantErr: false,
		},
		{
			name: "OK",
			args: args{
				csvfile:    "./data/sub/itemResults0.csv",
				outdir:     "out",
				categories: []string{"School", "YrLevel", "Domain"},
			},
			want:    []string{},
			wantErr: false,
		},
		{
			name: "OK",
			args: args{
				csvfile:    "./data/sub/itemResults111.csv",
				outdir:     "out",
				categories: []string{"School", "YrLevel", "Domain"},
			},
			want:    []string{},
			wantErr: false,
		},
		{
			name: "OK",
			args: args{
				csvfile:    "./data/sub/itemResults110.csv",
				outdir:     "out",
				categories: []string{"School", "YrLevel", "Domain"},
			},
			want:    []string{},
			wantErr: false,
		},
		{
			name: "OK",
			args: args{
				csvfile:    "./data/sub/itemResults101.csv",
				outdir:     "out",
				categories: []string{"School", "YrLevel", "Domain"},
			},
			want:    []string{},
			wantErr: false,
		},
		{
			name: "OK",
			args: args{
				csvfile:    "/home/qingmiao/Desktop/nrt-issue/csv-tool/data/sub/itemResults100.csv",
				outdir:     "out",
				categories: []string{"School", "YrLevel", "Domain"},
			},
			want:    []string{},
			wantErr: false,
		},
		// {
		// 	name: "OK",
		// 	args: args{
		// 		csvfile:    "./data/data.csv",
		// 		outdir:     "outmedium",
		// 		categories: []string{"School", "Domain", "YrLevel"},
		// 	},
		// 	want:    []string{},
		// 	wantErr: false,
		// },
		// {
		// 	name: "OK",
		// 	args: args{
		// 		csvfile:    "./data/big/itemResults.csv",
		// 		outdir:     "outbig",
		// 		categories: []string{"School", "Domain", "YrLevel"},
		// 	},
		// 	want:    []string{},
		// 	wantErr: false,
		// },
	}

	ForceSingleProc(true)
	Dir4NotSplittable("./out/ignore")
	StrictSchema(true)
	KeepCatHeaders(true)
	KeepIgnCatHeaders(true)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			splitfiles, ignoredfiles, _ := Split(tt.args.csvfile, tt.args.outdir, tt.args.categories...)
			fmt.Println(len(splitfiles))
			fmt.Println(len(ignoredfiles))
		})
	}

	fmt.Println(io.FileDirCount("out", true))
	fmt.Println(io.FileDirCount("outmedium", true))
	fmt.Println(io.FileDirCount("outbig", true))
}
