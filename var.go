package csvtool

import (
	"fmt"
	"strings"

	"github.com/cdutwhu/debog/fn"
	"github.com/cdutwhu/gotil/iter"
	"github.com/cdutwhu/gotil/judge"
	"github.com/cdutwhu/gotil/misc"
	jsontool "github.com/cdutwhu/json-tool"
	"github.com/digisan/gotk/io"
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
	sSplit      = strings.Split
	sJoin       = strings.Join
	sTrimSuffix = strings.TrimSuffix
	sTrim       = strings.Trim

	failOnErr       = fn.FailOnErr
	failP1OnErr     = fn.FailP1OnErr
	failOnErrWhen   = fn.FailOnErrWhen
	failP1OnErrWhen = fn.FailP1OnErrWhen
	warnOnErr       = fn.WarnOnErr
	enableLog2F     = fn.EnableLog2F
	mustWriteFile   = io.MustWriteFile
	mustAppendFile  = io.MustAppendFile
	iter2slc        = iter.Iter2Slc
	isContInts      = judge.IsContInts
	trackTime       = misc.TrackTime
	isValidJSON     = jsontool.IsValid
	jsonScalarSelX  = jsontool.ScalarSelX
)
