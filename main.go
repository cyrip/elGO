package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"

	"github.com/google/uuid"
	elastic "github.com/olivere/elastic/v7"
)

const ELASTIC_INDEX_NAME string = "cars"
const ELASTIC_URL = "http://127.0.0.1:9200"

var client *elastic.Client

type Car struct {
	//UUID5       string   `json:"string"`
	PlateNumber string   `json:"rendszam"`
	Owner       string   `json:"tulajdonos"`
	ValidUntil  string   `json:"forgalmi_ervenyes"`
	Data        []string `json:"adatok"`
}

type Elastic struct {
	indexName     string
	elasticClient *elastic.Client
}

func (this *Elastic) Init(indexName string) *elastic.Client {
	if this.elasticClient == nil {
		var err error
		this.elasticClient, err = elastic.NewClient(
			elastic.SetURL(ELASTIC_URL),
			elastic.SetSniff(false),
		)
		if err != nil {
			log.Fatal(err)
		}
		this.indexName = indexName
	}
	return this.elasticClient
}

func (this *Elastic) CreateIndex() {
	mapping := `{
		"settings": {
			"number_of_shards": 2,
			"number_of_replicas": 1
		},
		"mappings": {
			"properties": {
				"rendszam": { "type": "keyword" },
				"tulajdonos": { "type": "keyword" },
				"forgalmi_ervenyes": { "type": "date" },
				"adatok": { "type": "keyword" }
			}
		}
	}`

	// Create an index with the defined settings and mappings
	createIndex, err := this.elasticClient.CreateIndex(this.indexName).BodyString(mapping).Do(context.Background())
	if err != nil {
		log.Fatalf("Failed to create index: %s", err)
	}
	if !createIndex.Acknowledged {
		log.Fatal("Create index not acknowledged")
	}

	log.Println("Index created successfully")
}

func (this *Elastic) DeleteIndex() {
	deleteIndex, err := this.elasticClient.DeleteIndex(this.indexName).Do(context.Background())
	if err != nil {
		log.Println("Error deleting the index: %s", err)
		return
	}
	if !deleteIndex.Acknowledged {
		// Not acknowledged
		log.Println("Delete index not acknowledged")
	} else {
		fmt.Println("Index deleted")
	}
}

func (this *Elastic) GetAllDocuments() {
	// Initialize scrolling over documents
	scroll := this.elasticClient.Scroll(this.indexName).Size(100) // Adjust size as needed
	for {
		results, err := scroll.Do(context.Background())
		if err == io.EOF {
			log.Println("All documents retrieved")
			break
		}
		if err != nil {
			log.Fatalf("Error retrieving documents: %s", err)
		}

		// Iterate through results
		for _, hit := range results.Hits.Hits {
			var doc map[string]interface{}
			if err := json.Unmarshal(hit.Source, &doc); err != nil {
				log.Fatalf("Error deserializing document: %s", err)
			}
			// Process your document (doc) here
			log.Printf("Doc ID: %s, Doc: %+v\n", hit.Id, doc)
		}
	}
}

func (this *Elastic) Search3(term string) {
	query := elastic.NewBoolQuery().Should(
		elastic.NewRegexpQuery("rendszam", term),
		elastic.NewRegexpQuery("tulajdonos", term),
		elastic.NewRegexpQuery("adatok", term),
	)

	searchResult, err := this.elasticClient.Search().
		Index(this.indexName).
		Query(query).
		Pretty(true).
		Do(context.Background())
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}

	fmt.Printf("Query took %d milliseconds\n", searchResult.TookInMillis)
	fmt.Printf("Found %d documents\n", searchResult.TotalHits())

	for _, hit := range searchResult.Hits.Hits {
		var doc map[string]interface{}
		err := json.Unmarshal(hit.Source, &doc)
		if err != nil {
			log.Fatalf("Error deserializing hit to document: %s", err)
		}
		fmt.Printf("Document ID: %s, Fields: %+v\n", hit.Id, doc)
	}
}

func (this *Elastic) GetUUID(name string) string {
	namespaceDNS := uuid.NameSpaceDNS
	uuidV5 := uuid.NewSHA1(namespaceDNS, []byte(name))
	return uuidV5.String()
}

func (this *Elastic) AddDocument(doc Car) {
	uuid5 := this.GetUUID(doc.PlateNumber)
	indexResponse, err := this.elasticClient.Index().
		Index(this.indexName).
		BodyJson(doc).
		Id(uuid5).
		Do(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Indexed document %s to index %s\n", indexResponse.Id, indexResponse.Index)
}

func (this *Elastic) Test1(plateNumber string) {
	car := Car{
		//UUID5:       uuid5,
		PlateNumber: plateNumber,
		Owner:       "KZ",
		ValidUntil:  "2024-01-01",
		Data:        []string{"data1", "data2", "data3"},
	}
	this.AddDocument(car)
	this.Search3(".*ABC.*")
}

func main() {
	eClient := Elastic{}
	eClient.Init(ELASTIC_INDEX_NAME)
	//eClient.DeleteIndex()
	//eClient.CreateIndex()
	eClient.Test1("ABC-123")
	//eClient.CreateIndex(ELASTIC_INDEX_NAME)
}
