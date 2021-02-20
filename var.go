package csvtool

import (
	"fmt"
	"strings"

	"github.com/cdutwhu/debog/fn"
	"github.com/cdutwhu/gotil/io"
	"github.com/cdutwhu/n3-util/n3json"
)

var (
	fPln        = fmt.Println
	fEf         = fmt.Errorf
	fSf         = fmt.Sprintf
	sReplaceAll = strings.ReplaceAll

	failOnErr     = fn.FailOnErr
	failOnErrWhen = fn.FailOnErrWhen
	warnOnErr     = fn.WarnOnErr
	enableLog2F   = fn.EnableLog2F
	mustWriteFile = io.MustWriteFile

	jsonScalarSelX = n3json.ScalarSelX
)