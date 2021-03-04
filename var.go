package csvtool

import (
	"fmt"
	"strings"

	"github.com/cdutwhu/debog/fn"
	"github.com/cdutwhu/gotil/io"
	"github.com/cdutwhu/gotil/iter"
	"github.com/cdutwhu/gotil/judge"
	"github.com/cdutwhu/gotil/misc"
	"github.com/cdutwhu/gotil/rflx"
	jsontool "github.com/cdutwhu/json-tool"
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

	failOnErr      = fn.FailOnErr
	failP1OnErr    = fn.FailP1OnErr
	failOnErrWhen  = fn.FailOnErrWhen
	warnOnErr      = fn.WarnOnErr
	enableLog2F    = fn.EnableLog2F
	mustWriteFile  = io.MustWriteFile
	exist          = judge.Exist
	notexist       = judge.NotExist
	cvt2GSlc       = rflx.ToGeneralSlc
	iter2Slc       = iter.Iter2Slc
	trackTime      = misc.TrackTime
	isValidJSON    = jsontool.IsValid
	jsonScalarSelX = jsontool.ScalarSelX
)
