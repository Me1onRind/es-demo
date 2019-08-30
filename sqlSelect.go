package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/Me1onRind/util"
	"github.com/elastic/go-elasticsearch/v6"
	"github.com/elastic/go-elasticsearch/v6/esapi"
	"os"
	"time"
)

func checkErr(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
}

func main() {
	cfg := elasticsearch.Config{
		Addresses: []string{
			"http://172.16.208.78:9200",
		},
	}
	c, err := elasticsearch.NewClient(cfg)
	checkErr(err)
	query := map[string]interface{}{
		"query": "select count(*) from test where uid in (" + util.JoinInts(getUids(), ",") + ")  and day >= 20190801 and day <= 20190820 and field1 = 'abc0' and field2 in (0, 1, 3) group by uid",
	}

	//fmt.Println(query["query"])
	var buf bytes.Buffer
	err = json.NewEncoder(&buf).Encode(query)
	checkErr(err)
	req := esapi.XPackSQLQueryRequest{
		Body: &buf,
	}
	begin := time.Now()
	//res, err := req.Do(context.Background(), c)
	//checkErr(err)
	//fmt.Println(res)
	_, err = req.Do(context.Background(), c)
	checkErr(err)
	fmt.Println(time.Since(begin))

}

func getUids() []int {
	var ret []int
	uid := 20000000
	for i := 0; i < 2000; i++ {
		ret = append(ret, uid+i*2)
	}
	return ret
}
