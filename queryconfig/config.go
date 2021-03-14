package queryconfig

// QueryConfig :
type QueryConfig struct {
	Query []struct {
		Name       string
		CsvPath    string
		OutCsvPath string
		IncColMode bool
		HdrNames   []string
		RelaOfCond string
		Cond       []struct {
			Header          string
			Value           interface{}
			ValueType       string
			RelaOfItemValue string
		}
	}
}
