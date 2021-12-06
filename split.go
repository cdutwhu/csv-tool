package csvtool

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/digisan/gotk/generics/ts"
)

var (
	basename       string
	csvdir         string
	mtx            = &sync.Mutex{}
	schema         []string
	nSchema        int
	keepCatHdr     bool
	keepIgnCatHdr  bool
	outdir         string
	splitfiles     []string
	ignoredfiles   []string
	parallel       = false
	mustSingleProc = false
	fileIgnoredOut = ""
	strictSchema   = false
)

// KeepCatHeaders :
func KeepCatHeaders(keep bool) {
	keepCatHdr = keep
}

// KeepIgnCatHeaders :
func KeepIgnCatHeaders(keep bool) {
	keepIgnCatHdr = keep
}

// Dir4NotSplittable : in LooseMode, only take the last path seg for dump folder
func Dir4NotSplittable(dir string) {
	fileIgnoredOut = dir
}

// StrictSchema :
func StrictSchema(strict bool) {
	strictSchema = strict
}

// ForceNoParallel :
func ForceSingleProc(sp bool) {
	mustSingleProc = sp
}

// Split : return (splitfiles, ignoredfiles, error)
func Split(csvfile, outputdir string, categories ...string) ([]string, []string, error) {

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
		return nil, nil, fEf("%v @ %s", err, csvfile)
	}

	// --------------- strict schema check --------------- //
	headers, nRow, err := FileInfo(csvfile)
	if err != nil {
		return nil, nil, fEf("%v @ %s", err, csvfile)
	}
	if strictSchema && len(fileIgnoredOut) > 0 {
		if !ts.Superset(headers, schema) || nRow == 0 {

			nsCsvFile, _ := relPath(csvfile, false)
			nsCsvFile = filepath.Join(fileIgnoredOut, nsCsvFile)

			// relPath output likes '../***' is not working with filepath.Join
			// manually put nsCsvFile under fileIgnoredOut.
			if !sContains(nsCsvFile, fileIgnoredOut+"/") {
				nsCsvFile = filepath.Join(fileIgnoredOut, nsCsvFile)
			}

			if keepIgnCatHdr {
				mustWriteFile(nsCsvFile, in)
			} else {
				mustCreateDir(filepath.Dir(nsCsvFile))
				fw, err := os.OpenFile(nsCsvFile, os.O_WRONLY|os.O_CREATE, 0666)
				failOnErr("%v @ %s", err, nsCsvFile)
				Subset(in, false, schema, false, nil, fw)
				fw.Close()
			}

			return []string{}, []string{nsCsvFile}, nil
		}
	}

	// --------------- parallel set --------------- //
	parallel = false
	if !mustSingleProc && len(in) < 1024*1024*10 {
		parallel = true
	}
	// fmt.Printf("%s running on parallel? %v\n", csvfile, parallel)

	splitfiles = []string{}
	ignoredfiles = []string{}
	return splitfiles, ignoredfiles, split(0, in, outdir)
}

func split(rl int, in []byte, prevpath string, pCatItems ...string) error {

	if rl >= nSchema {
		return nil
	}

	cat := schema[rl]
	rl++

	rmHdrGrp := []string{cat}
	if keepCatHdr {
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

				fileIgnoredOutInfo := fSf("%s(missing %s)", filepath.Base(fileIgnoredOut), cat)
				nsCsvDir, _ := relPath(csvdir, false)
				fileIgnoredInfo := fSf("%s(%s).csv", sTrimSuffix(basename, ".csv"), nsCsvDir)
				fileIgnoredInfo = sReplaceAll(fileIgnoredInfo, "/", "~")
				nsCsvFile := filepath.Join(prevpath, fileIgnoredOutInfo, fileIgnoredInfo)

				if keepIgnCatHdr {
					mustWriteFile(nsCsvFile, in)
				} else {
					mustCreateDir(filepath.Dir(nsCsvFile))
					fw, err := os.OpenFile(nsCsvFile, os.O_WRONLY|os.O_CREATE, 0666)
					failOnErr("%v @ %s", err, nsCsvFile)
					Subset(in, false, schema, false, nil, fw)
					fw.Close()
				}

				ignoredfiles = append(ignoredfiles, nsCsvFile)
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
				splitfiles = append(splitfiles, outcsv)
			}

			split(rl, wBuf.Bytes(), filepath.Dir(outcsv), append(pCatItems, catItem)...)
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
					splitfiles = append(splitfiles, outcsv)
					mtx.Unlock()
				}

				split(rl, wBuf.Bytes(), filepath.Dir(outcsv), append(pCatItems, catItem)...)

			}(catItem)
		}

		wg.Wait()
	}

	return nil
}
