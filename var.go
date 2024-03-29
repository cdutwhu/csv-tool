package csvtool

import (
	"fmt"
	"strings"

	"github.com/cdutwhu/debog/fn"
	"github.com/cdutwhu/gotil/judge"
	jsontool "github.com/cdutwhu/json-tool"
	"github.com/digisan/gotk"
	"github.com/digisan/gotk/io"
	"github.com/digisan/gotk/iter"
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

	failOnErr       = fn.FailOnErr
	failP1OnErr     = fn.FailP1OnErr
	failOnErrWhen   = fn.FailOnErrWhen
	failP1OnErrWhen = fn.FailP1OnErrWhen
	warnOnErr       = fn.WarnOnErr
	enableLog2F     = fn.EnableLog2F
	mustWriteFile   = io.MustWriteFile
	mustAppendFile  = io.MustAppendFile
	fileExists      = io.FileExists
	iter2slc        = iter.Iter2Slc
	isContInts      = judge.IsContInts
	trackTime       = gotk.TrackTime
	isValidJSON     = jsontool.IsValid
	jsonScalarSelX  = jsontool.ScalarSelX
)
