package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v6"
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
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": map[string]interface{}{
					"terms": map[string]interface{}{
						"uid": getUids(),
					},
				},
				"filter": map[string]interface{}{
					"range": map[string]interface{}{
						"day": map[string]int{
							"gt": 20190805,
							"lt": 20190820,
						},
					},
				},
			},
		},
		"size": 0,
		"aggs": map[string]interface{}{
			"group": map[string]interface{}{
				"terms": map[string]interface{}{
					"field": "uid",
					"size":  1,
				},
				"aggs": map[string]interface{}{
					"reted": map[string]interface{}{
						"top_hits": map[string]interface{}{
							"size": 1,
						},
					},
				},
			},
		},
	}

	var buf bytes.Buffer
	err = json.NewEncoder(&buf).Encode(query)
	checkErr(err)

	buf.Reset()
	buf.Write([]byte("select count(*) from test"))

	begin := time.Now()
	res, err := c.Search(
		c.Search.WithIndex("test"),
		c.Search.WithBody(&buf),
	)
	fmt.Println(time.Since(begin))
	defer res.Body.Close()

	checkErr(err)
	fmt.Println(res)

}

func getUids() []int {
	var ret []int
	uid := 20000000
	for i := 0; i < 2000; i++ {
		ret = append(ret, uid+i*2)
	}
	return ret
}
