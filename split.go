package csvtool

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/digisan/gotk/slice/ts"
)

var (
	basename       string
	csvdir         string
	mtx            = &sync.Mutex{}
	schema         []string
	nSchema        int
	outdir         string
	outfiles       []string
	parallel       = false
	mustSingleProc = false
	fileIgnoredOut = ""
)

// Dir4NotSplittable :
func Dir4NotSplittable(dir string) {
	fileIgnoredOut = dir
}

// ForceNoParallel :
func ForceSingleProc(sp bool) {
	mustSingleProc = sp
}

// Split :
func Split(csvfile, outputdir string, keepcat bool, categories ...string) ([]string, error) {

	basename = filepath.Base(csvfile)
	csvdir = filepath.Dir(csvfile)

	schema = categories
	nSchema = len(schema)

	if outputdir == "" {
		outdir = "./" + sTrimSuffix(basename, ".csv") + "/"
	} else {
		outdir = sTrimSuffix(outputdir, "/") + "/"
	}

	in, err := os.ReadFile(csvfile)
	if err != nil {
		return nil, err
	}

	// --------------- parallel set --------------- //
	parallel = false
	if !mustSingleProc && len(in) < 1024*1024*10 {
		parallel = true
	}
	// fmt.Printf("%s running on parallel? %v\n", csvfile, parallel)

	outfiles = []string{}
	return outfiles, split(0, in, outdir, keepcat)
}

func split(rl int, in []byte, prevpath string, keepcat bool, pCatItems ...string) error {

	if rl >= nSchema {
		return nil
	}

	cat := schema[rl]
	rl++

	rmHdrGrp := []string{cat}
	if keepcat {
		rmHdrGrp = nil
	}

	_, rows, err := Subset(in, true, []string{cat}, false, nil, nil)
	if err != nil {
		return err
	}

	// --------------- not splittable --------------- //
	// empty / empty content / missing needed categories
	if len(fileIgnoredOut) > 0 {
		if func() bool {
			mtx.Lock()
			defer mtx.Unlock()
			if len(rows) == 0 || (len(rows) > 0 && len(sTrim(rows[0], " \t")) == 0) {
				fileIgnoredOutInfo := fSf("%s(missing %s)", fileIgnoredOut, cat)
				fileIgnoredInfo := fSf("%s(%s).csv", sTrimSuffix(basename, ".csv"), csvdir)
				fileIgnoredInfo = sReplaceAll(fileIgnoredInfo, "/", "~")
				nsCsvFile := filepath.Join(prevpath, fileIgnoredOutInfo, fileIgnoredInfo)

				// subsetting ignored files
				if !keepcat {
					mustCreateDir(filepath.Dir(nsCsvFile))
					fw, err := os.OpenFile(nsCsvFile, os.O_WRONLY|os.O_CREATE, 0666)
					failOnErr("%v @ %s", err, nsCsvFile)
					Subset(in, false, schema, false, nil, fw)
					fw.Close()
				} else {
					mustWriteFile(nsCsvFile, in)
				}

				outfiles = append(outfiles, nsCsvFile)
				return true
			}
			return false
		}() {
			return nil
		}
	}
	// --------------- end --------------- //

	unirows := ts.MkSet(rows...)
	unirows = ts.FM(unirows, func(i int, e string) bool { return len(sTrim(e, " \t")) > 0 }, nil)

	// Safe Mode, But Slow //
	if !parallel {

		for _, catItem := range unirows {

			outcsv := outdir
			for _, pcItem := range pCatItems {
				outcsv += pcItem + "/"
			}
			outcsv += catItem + "/" + basename

			wBuf := &bytes.Buffer{}

			Query(
				in,
				false,
				rmHdrGrp,
				'&',
				[]Condition{{Hdr: cat, Val: catItem, ValTyp: "string", Rel: "="}},
				io.Writer(wBuf),
			)

			if rl == nSchema {
				mustWriteFile(outcsv, wBuf.Bytes())
				outfiles = append(outfiles, outcsv)
			}

			split(rl, wBuf.Bytes(), filepath.Dir(outcsv), keepcat, append(pCatItems, catItem)...)
		}
	}

	// Whole Linux Exhausted When Running On Big Data //
	if parallel {

		wg := &sync.WaitGroup{}
		wg.Add(len(unirows))

		for _, catItem := range unirows {

			go func(catItem string) {
				defer wg.Done()

				outcsv := outdir
				for _, pcItem := range pCatItems {
					outcsv += pcItem + "/"
				}
				outcsv += catItem + "/" + basename

				wBuf := &bytes.Buffer{}

				Query(
					in,
					false,
					rmHdrGrp,
					'&',
					[]Condition{{Hdr: cat, Val: catItem, ValTyp: "string", Rel: "="}},
					io.Writer(wBuf),
				)

				if rl == nSchema {
					mtx.Lock()
					mustWriteFile(outcsv, wBuf.Bytes())
					outfiles = append(outfiles, outcsv)
					mtx.Unlock()
				}

				split(rl, wBuf.Bytes(), filepath.Dir(outcsv), keepcat, append(pCatItems, catItem)...)

			}(catItem)
		}

		wg.Wait()
	}

	return nil
}
