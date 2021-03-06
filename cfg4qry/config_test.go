package cfg4qry

import (
	"fmt"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/cdutwhu/debog/fn"
	"github.com/davecgh/go-spew/spew"
)

func TestQueryConfig(t *testing.T) {
	cfg := &QueryConfig{}
	_, err := toml.DecodeFile("./query.toml", cfg)
	fn.FailOnErr("%v", err)
	fmt.Println("-------------------------------")
	spew.Dump(cfg.Query[0])
	fmt.Println("-------------------------------")
	spew.Dump(cfg.Query[1])
	fmt.Println("-------------------------------")
}
