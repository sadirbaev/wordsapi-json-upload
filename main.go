package main

import (
	"encoding/json"
	"fmt"
	"github.com/olivere/elastic/v7"
	"github.com/spf13/viper"
	"golang.org/x/net/context"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

type Definitions struct {
	Examples []string `json:"examples"`
}

type Words struct {
	Definitions []Definitions `json:"definitions"`
}

type Document struct {
	Word     string   `json:"word"`
	Examples []string `json:"examples"`
}


func init() {
	viper.SetConfigFile(`config.json`)
	err := viper.ReadInConfig()
	if err != nil {
		panic(err)
	}
}

func main() {

	es, err := getESClient()
	if err != nil {
		log.Println("Connection failed to Elastic Search")
		log.Fatal(err)
	}
	log.Println("Successfully connected to Elastic Search")

	jsonFile, err := os.Open(viper.GetString("filename"))
	if err != nil {
		log.Println("Error occurred while opening the file: " + viper.GetString("filename"))
		log.Fatal(err)
	}
	log.Println("Successfully Opened " + viper.GetString("filename"))
	defer func() {
		err = jsonFile.Close()
		if err != nil {
			log.Println("Error occurred while closing the file: " + viper.GetString("filename"))
			log.Fatal(err)
		}
	}()



	byteValue, err := ioutil.ReadAll(jsonFile)
	if err != nil {
		log.Println("Error occurred while reading all lines to end of file: " + viper.GetString("filename"))
		log.Fatal(err)
	}

	// container
	c := make(map[string]Words)

	err = json.Unmarshal(byteValue, &c)
	if err != nil {
		log.Println("Error occurred while parsing json to struct")
		log.Fatal(err)
	}
	for k, v := range c {
		var examples = []string{}

		if v.Definitions != nil {
			for _, definition := range v.Definitions {
				examples = append(examples, definition.Examples...)
			}
		}
		err = createDoc(es, k, examples)
		if err != nil {
			log.Println(fmt.Sprintf("%s: %s", k, err))
		}
	}
}

func createDoc(es *elastic.Client, k string, v []string) (err error) {
	index := viper.GetString(`elasticsearch.index`)
	body, err := json.Marshal(Document{k, v})
	if err != nil {
		return
	}
	contentString := string(body)
	doc := strings.Replace(k, " ", "_", -1)
	_, err = es.Index().
		Index(index).
		OpType("create").
		BodyString(contentString).
		Id(doc).
		Do(context.Background())
	return
}

func getESClient() (*elastic.Client, error) {
	client, err := elastic.NewClient(elastic.SetURL(fmt.Sprintf("http://%s:%s", viper.GetString("elasticsearch.host"), viper.GetString("elasticsearch.port"))),
		elastic.SetSniff(false),
		elastic.SetHealthcheck(false))
	return client, err
}
