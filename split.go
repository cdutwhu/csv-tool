package csvtool

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/digisan/gotk/slice/ts"
)

// Split :
func Split(csvfile, outdir string, keepcat bool, categories ...string) error {

	basename := filepath.Base(csvfile)
	if outdir == "" {
		outdir = "./" + sTrimSuffix(basename, ".csv") + "/"
	} else {
		outdir = sTrimSuffix(outdir, "/") + "/"
	}
	return split(0, csvfile, outdir, basename, keepcat, categories)

}

func split(rl int, csvfile, outdir, basename string, keepcat bool, categories []string, pCatItems ...string) error {
	if rl >= len(categories) {
		return nil
	}

	defer func() {
		if rl > 1 && rl <= len(categories) {
			os.RemoveAll(csvfile)
		}
	}()

	cat := categories[rl]
	rl++

	rmHdrGrp := []string{cat}
	if keepcat {
		rmHdrGrp = nil
	}

	_, rows, err := Subset(csvfile, true, []string{cat}, false, nil, "")
	if err != nil {
		return err
	}

	unirows := ts.MkSet(rows...)

	wg := &sync.WaitGroup{}
	wg.Add(len(unirows))

	for _, catItem := range unirows {

		go func(wg *sync.WaitGroup, catItem string) {
			defer wg.Done()

			outcsv := outdir
			for _, pcItem := range pCatItems {
				outcsv += pcItem + "/"
			}
			outcsv += catItem + "/" + basename
			// fmt.Println(outcsv)

			_, _, err := Query(csvfile,
				false,
				rmHdrGrp,
				'&',
				[]Condition{{Hdr: cat, Val: catItem, ValTyp: "string", Rel: "="}},
				outcsv,
				nil,
			)
			if err != nil {
				panic(err)
			}

			split(rl, outcsv, outdir, basename, keepcat, categories, append(pCatItems, catItem)...)

		}(wg, catItem)
	}

	wg.Wait()

	return nil
}
