package csvtool

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"sync"

	gotkio "github.com/digisan/gotk/io"
	"github.com/digisan/gotk/slice/ts"
)

var (
	mtx        = &sync.Mutex{}
	schema     []string
	nSchema    int
	outfiles   []string
	parallel   = false
	noParallel = false
	notsplit   = "./notsplit/"
)

// NotSplittableDir :
func NotSplittableDir(dir string) {
	notsplit, _ = gotkio.AbsPath(dir, false)
	mustCreateDir(notsplit)
}

// ForceNoParallel :
func ForceSingleProc(sp bool) {
	noParallel = sp
}

// Split :
func Split(csvfile, outdir string, keepcat bool, categories ...string) ([]string, error) {

	schema = categories
	nSchema = len(schema)

	basename := filepath.Base(csvfile)
	if outdir == "" {
		outdir = "./" + sTrimSuffix(basename, ".csv") + "/"
	} else {
		outdir = sTrimSuffix(outdir, "/") + "/"
	}

	in, err := os.ReadFile(csvfile)
	if err != nil {
		return nil, err
	}

	// --------------- not splittable --------------- //
	// empty file / empty content csv / no categories csv
	_, rows, err := Subset(in, true, categories, false, nil, nil)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 || (len(rows) > 0 && sTrim(rows[0], " \t") == "") {
		csvfile = filepath.Join(notsplit, filepath.Base(csvfile))
		mustWriteFile(csvfile, in)
		return []string{csvfile}, nil
	}

	// --------------- parallel set --------------- //
	parallel = false
	if !noParallel && len(in) < 1024*1024*10 {
		parallel = true
	}
	// fmt.Printf("%s running on parallel? %v\n", csvfile, parallel)

	// split, if no suitable position to put it in, throw it to <notsplit>
	outfiles = []string{}
	err = split(0, in, outdir, basename, keepcat)
	if len(outfiles) == 0 {
		csvfile = filepath.Join(notsplit, filepath.Base(csvfile))
		mustWriteFile(csvfile, in)
		return []string{csvfile}, nil
	}

	return outfiles, err
}

func split(rl int, in []byte, outdir, basename string, keepcat bool, pCatItems ...string) error {

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

	unirows := ts.MkSet(rows...)

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

			split(rl, wBuf.Bytes(), outdir, basename, keepcat, append(pCatItems, catItem)...)
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

				split(rl, wBuf.Bytes(), outdir, basename, keepcat, append(pCatItems, catItem)...)

			}(catItem)
		}

		wg.Wait()

	}

	return nil
}
