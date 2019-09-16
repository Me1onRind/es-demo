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
)

var c *elasticsearch.Client

func init() {
	var err error
	config := elasticsearch.Config{}
	config.Addresses = []string{"http://172.16.208.78:9200"}
	c, err = elasticsearch.NewClient(config)
	checkError(err)
	//res, err := c.Info()
	//defer res.Body.Close()
	//fmt.Println(res.String())
}

func checkError(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func createTable() {
	req := esapi.IndicesCreateRequest{
		Index: "test_index",
	}
	res, err := req.Do(context.Background(), c)
	checkError(err)
	defer res.Body.Close()
	fmt.Println(res.String())
}

func deleteTable() {
	req := esapi.IndicesDeleteRequest{
		Index: []string{"test_index"},
	}
	res, err := req.Do(context.Background(), c)
	checkError(err)
	defer res.Body.Close()
	fmt.Println(res.String())
}

func insertSingle() {
	body := map[string]interface{}{
		"num": 0,
		"str": "test",
	}
	jsonBody, _ := json.Marshal(body)

	req := esapi.CreateRequest{
		Index:        "test_index",
		DocumentType: "test_type",
		DocumentID:   "test_1",
		Body:         bytes.NewReader(jsonBody),
	}
	res, err := req.Do(context.Background(), c)
	checkError(err)
	defer res.Body.Close()
	fmt.Println(res.String())
}

func insertBatch() {
	var bodyBuf bytes.Buffer
	for i := 2; i < 10; i++ {
		createLine := map[string]interface{}{
			"create": map[string]interface{}{
				"_index": "test_index",
				"_id":    "test_" + strconv.Itoa(i),
				"_type":  "test_type",
			},
		}
		jsonStr, _ := json.Marshal(createLine)
		bodyBuf.Write(jsonStr)
		bodyBuf.WriteByte('\n')

		body := map[string]interface{}{
			"num": i % 3,
			"str": "test" + strconv.Itoa(i),
		}
		jsonStr, _ = json.Marshal(body)
		bodyBuf.Write(jsonStr)
		bodyBuf.WriteByte('\n')
	}

	req := esapi.BulkRequest{
		Body: &bodyBuf,
	}
	res, err := req.Do(context.Background(), c)
	checkError(err)
	defer res.Body.Close()
	fmt.Println(res.String())
}

func selectBySql() {
	query := map[string]interface{}{
		"query": "select count(*) as cnt, max(str) as s, num from test_index where num > 1 group by num, str limit 2",
	}
	jsonBody, _ := json.Marshal(query)
	req := esapi.XPackSQLQueryRequest{
		Body: bytes.NewReader(jsonBody),
	}
	res, err := req.Do(context.Background(), c)
	checkError(err)
	defer res.Body.Close()
	fmt.Println(res.String())
}

func main() {
	//deleteTable()
	//createTable()
	//insertSingle()
	//insertBatch()
	selectBySql()
}
