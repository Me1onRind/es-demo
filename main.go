package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v6"
	"github.com/elastic/go-elasticsearch/v6/esapi"
	"os"
	"strconv"
	"strings"
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
	//for i := 0; i < 20; i++ {
	//insert(c, i)
	//}
	//create(c)
	//setMapping(c)
	//delete(c)
	//insert(c)
	//return

	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": struct{}{},
		},
		"size": 0,
		"aggs": map[string]interface{}{
			"group": map[string]interface{}{
				"terms": map[string]string{
					"field": "key",
				},
			},
		},
	}

	var buf bytes.Buffer
	err = json.NewEncoder(&buf).Encode(query)
	checkErr(err)

	res, err := c.Search(
		c.Search.WithIndex("test"),
		c.Search.WithBody(&buf),
	)

	checkErr(err)
	fmt.Println(res)

}

func insert(c *elasticsearch.Client, i int) {
	req := esapi.IndexRequest{
		Index:      "test",
		DocumentID: "uuid" + strconv.Itoa(i),
		Body:       strings.NewReader("{\"key\":\"value\",\"field\":123}"),
		Refresh:    "true",
	}

	res, err := req.Do(context.Background(), c)
	checkErr(err)
	fmt.Println(res.String())
}

func setMapping(c *elasticsearch.Client) {
	mapping := map[string]interface{}{
		"properties": map[string]interface{}{
			"key": map[string]string{
				"type": "keyword",
			},
		},
	}

	var buf bytes.Buffer
	err := json.NewEncoder(&buf).Encode(mapping)
	checkErr(err)

	req := esapi.IndicesPutMappingRequest{
		Index:        []string{"test"},
		DocumentType: "_doc",
		Body:         &buf,
	}

	res, err := req.Do(context.Background(), c)
	checkErr(err)
	fmt.Println(res)
}

func delete(c *elasticsearch.Client) {
	req := esapi.IndicesDeleteRequest{
		Index: []string{"test"},
	}
	res, err := req.Do(context.Background(), c)
	checkErr(err)
	fmt.Println(res)
}

func create(c *elasticsearch.Client) {
	req := esapi.IndicesCreateRequest{
		Index: "test",
	}
	res, err := req.Do(context.Background(), c)
	checkErr(err)
	fmt.Println(res)
}
