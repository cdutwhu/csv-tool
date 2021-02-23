package csvtool

import (
	"fmt"
	"strings"

	"github.com/cdutwhu/debog/fn"
	"github.com/cdutwhu/gotil/io"
	jsontool "github.com/cdutwhu/json-tool"
)

var (
	fPln        = fmt.Println
	fEf         = fmt.Errorf
	fSf         = fmt.Sprintf
	sReplaceAll = strings.ReplaceAll
	sHasSuffix  = strings.HasSuffix
	sRepeat     = strings.Repeat
	sContains   = strings.Contains
	sSplit      = strings.Split
	sJoin       = strings.Join

	failOnErr      = fn.FailOnErr
	failOnErrWhen  = fn.FailOnErrWhen
	warnOnErr      = fn.WarnOnErr
	enableLog2F    = fn.EnableLog2F
	mustWriteFile  = io.MustWriteFile
	isValidJSON    = jsontool.IsValid
	jsonScalarSelX = jsontool.ScalarSelX
)
