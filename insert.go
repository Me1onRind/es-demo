package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v6"
	"github.com/elastic/go-elasticsearch/v6/esapi"
	"io"
	"os"
	"strconv"
)

type Data struct {
	Day    int    `json:"day"`
	Uid    int    `json:"uid"`
	Field1 string `json:"field1"`
	Field2 int    `json:"field2"`
	Field3 int    `json:"field3"`
	Field4 int    `json:"field4"`
	Field5 int    `json:"field5"`
	Field6 int    `json:"field6"`
	Field7 int    `json:"field7"`
}

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
	delete(c)
	create(c)

	day := 20190801
	split := int(10000000 / 30)
	var jsonBuf, bodyBuf bytes.Buffer
	for i := 0; i < 10000000; i++ {
		for j := 0; j < 5000; j, i = j+1, i+1 {
			d := day + int(i/split)
			bodyBuf.Write([]byte("{\"create\":{\"_index\":\"test\",\"_id\":" + strconv.Itoa(i) + "}\n"))
			data := getData(i, d)
			json.NewEncoder(&jsonBuf).Encode(data)
			bodyBuf.Write(jsonBuf.Bytes())
			jsonBuf.Reset()
		}
		i--
		insert(c, &bodyBuf)
		bodyBuf.Reset()
		fmt.Println(i)
	}
}

func insert(c *elasticsearch.Client, body io.Reader) {
	req := esapi.BulkRequest{
		Index:        "test",
		DocumentType: "_doc",
		Body:         body,
	}
	res, err := req.Do(context.Background(), c)
	if err != nil {
		fmt.Println(res)
	}
	checkErr(err)
	res.Body.Close()
}

func delete(c *elasticsearch.Client) {
	req := esapi.IndicesDeleteRequest{
		Index: []string{"test"},
	}
	res, err := req.Do(context.Background(), c)
	checkErr(err)
	fmt.Println(res)
}

func getField1(i int) string {
	return "abc" + strconv.Itoa(i%20)
}

func getField2(i int) int {
	return i % 10
}

func getField3(i int) int {
	return i % 5
}

func getUid(i int) int {
	return 20000000 + i%5000
}

func getData(i int, day int) Data {
	return Data{
		Day:    day,
		Uid:    getUid(i),
		Field1: getField1(i),
		Field2: getField2(i),
		Field3: getField3(i),
	}
}

func create(c *elasticsearch.Client) {
	req := esapi.IndicesCreateRequest{
		Index: "test",
	}
	req.Do(context.Background(), c)
}
