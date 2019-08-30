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
			"http://127.0.0.1:9200",
		},
	}
	c, err := elasticsearch.NewClient(cfg)
	checkErr(err)

	query := map[string]interface{}{
		"query": map[string]interface{}{
			//"match_all": struct{}{},
			"terms": map[string]interface{}{
				//"_id": []string{"50001"},
				"uid": getUids(),
			},
			//"range": map[string]interface{}{
			//"day": map[string]int{
			//"gt": 20190805,
			//"lt": 20190820,
			//},
			//},
		},
		"size": 200,
		"aggs": map[string]interface{}{
			"group": map[string]interface{}{
				"terms": map[string]string{
					"field": "uid",
				},
			},
		},
	}

	var buf bytes.Buffer
	err = json.NewEncoder(&buf).Encode(query)
	checkErr(err)

	begin := time.Now()
	res, err := c.Search(
		c.Search.WithIndex("test"),
		c.Search.WithBody(&buf),
	)
	fmt.Println(time.Since(begin))
	defer res.Body.Close()

	checkErr(err)
	//fmt.Println(res)

}

func getUids() []int {
	var ret []int
	uid := 20000000
	for i := 0; i < 2000; i++ {
		ret = append(ret, uid+i*2)
	}
	return ret
}
