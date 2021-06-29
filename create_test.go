package csvtool

import (
	"testing"

	"github.com/digisan/gotk/slice/ts"
)

func fortest() {
	headersC := ts.MkSet("a", "b", "c", "a", "c", "d")
	fPln(headersC)
}

func Test_fortest(t *testing.T) {
	tests := []struct {
		name string
	}{
		{
			name: "OK",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fortest()
		})
	}
}

func TestCreate(t *testing.T) {
	type args struct {
		outcsv   string
		hdrNames []string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		// TODO: Add test cases.
		{
			name:    "OK",
			args:    args{outcsv: "./out/create1.csv", hdrNames: nil},
			want:    "",
			wantErr: true,
		},
		{
			name:    "OK",
			args:    args{outcsv: "./out/create1.csv", hdrNames: []string{}},
			want:    "",
			wantErr: false,
		},
		{
			name:    "OK",
			args:    args{outcsv: "./out/create1.csv", hdrNames: []string{"eee", ""}},
			want:    "eee,",
			wantErr: false,
		},
		{
			name:    "OK",
			args:    args{outcsv: "./out/create2.csv", hdrNames: []string{"h1", "h2", "h3,n\"ame"}},
			want:    "h1,h2,\"h3,n\"\"ame\"",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Create(tt.args.outcsv, tt.args.hdrNames...)
			if (err != nil) != tt.wantErr {
				t.Errorf("Create() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Create() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAppendRows(t *testing.T) {
	type args struct {
		csvpath  string
		validate bool
		rows     []string
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
		{
			name: "OK",
			args: args{
				csvpath:  "./out/create2.csv",
				validate: true,
				rows: []string{
					`0,"Ahmad,Ahmad","Ahmad,""Ahmad1"`,
					`0,"Ahmad,Ahmad","Ahmad,""Ahmad1"`,
					`0,"Ahmad,Ahmad","Ahmad,""Ahmad1"`,
				}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			AppendRows(tt.args.csvpath, tt.args.validate, tt.args.rows...)
		})
	}
}

func TestCombine(t *testing.T) {
	type args struct {
		csvfileA        string
		csvfileB        string
		linkHeaders     []string
		onlyKeepLinkRow bool
		outcsv          string
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
		{
			name: "OK",
			args: args{
				csvfileA:        "./data/Modules.csv",
				csvfileB:        "./data/Questions.csv",
				linkHeaders:     []string{"module_version_id"},
				onlyKeepLinkRow: true,
				outcsv:          "./out/combine.csv",
			},
		},
		{
			name: "OK",
			args: args{
				csvfileA:        "./data/Modules.csv",
				csvfileB:        "./data/Questions.csv",
				linkHeaders:     []string{"module_version_id"},
				onlyKeepLinkRow: false,
				outcsv:          "./out/combine1.csv",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Combine(tt.args.csvfileA, tt.args.csvfileB, tt.args.linkHeaders, tt.args.onlyKeepLinkRow, tt.args.outcsv)
		})
	}
}
