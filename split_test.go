package csvtool

import (
	"reflect"
	"testing"
)

func Test_split(t *testing.T) {
	type args struct {
		rl         int
		csvfile    string
		outdir     string
		basename   string
		keepcat    bool
		categories []string
		pCatItems  []string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := split(tt.args.rl, tt.args.csvfile, tt.args.outdir, tt.args.basename, tt.args.keepcat, tt.args.categories, tt.args.pCatItems...); (err != nil) != tt.wantErr {
				t.Errorf("split() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSplit(t *testing.T) {
	type args struct {
		csvfile    string
		outdir     string
		keepcat    bool
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
				csvfile:    "./data/itemResults.csv",
				outdir:     "out",
				keepcat:    true,
				categories: []string{"School", "Domain", "YrLevel"},
			},
			wantErr: false,
		},
		{
			name: "OK",
			args: args{
				csvfile:    "./data/itemResults1.csv",
				outdir:     "out1",
				keepcat:    false,
				categories: []string{"School", "Domain"},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Split(tt.args.csvfile, tt.args.outdir, tt.args.keepcat, tt.args.categories...)
			if (err != nil) != tt.wantErr {
				t.Errorf("Split() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Split() = %v, want %v", got, tt.want)
			}
		})
	}
}
