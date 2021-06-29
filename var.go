package csvtool

import (
	"fmt"
	"strings"

	"github.com/digisan/gotk"
	fd "github.com/digisan/gotk/filedir"
	"github.com/digisan/gotk/io"
	"github.com/digisan/gotk/iter"
	jt "github.com/digisan/json-tool"
	lk "github.com/digisan/logkit"
)

var (
	fPln        = fmt.Println
	fEf         = fmt.Errorf
	fSf         = fmt.Sprintf
	sReplaceAll = strings.ReplaceAll
	sHasPrefix  = strings.HasPrefix
	sHasSuffix  = strings.HasSuffix
	sRepeat     = strings.Repeat
	sContains   = strings.Contains
	sJoin       = strings.Join
	sTrimSuffix = strings.TrimSuffix
	sTrim       = strings.Trim

	enableLog2F     = lk.Log2F
	failOnErr       = lk.FailOnErr
	failP1OnErr     = lk.FailP1OnErr
	failOnErrWhen   = lk.FailOnErrWhen
	failP1OnErrWhen = lk.FailP1OnErrWhen
	warnOnErr       = lk.WarnOnErr
	mustWriteFile   = io.MustWriteFile
	mustAppendFile  = io.MustAppendFile
	mustCreateDir   = io.MustCreateDir
	fileExists      = fd.FileExists
	relPath         = fd.RelPath
	iter2slc        = iter.Iter2Slc
	isContInts      = gotk.IsContInts
	trackTime       = gotk.TrackTime
	isValidJSON     = jt.IsValid
	jsonScalarSelX  = jt.ScalarSelX
)
