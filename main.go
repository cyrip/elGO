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

const INDEX_NAME string = "cars"

var client *elastic.Client

type Car struct {
	//UUID5       string   `json:"string"`
	PlateNumber string   `json:"rendszam"`
	Owner       string   `json:"tulajdonos"`
	ValidUntil  string   `json:"forgalmi_ervenyes"`
	Data        []string `json:"adatok"`
}

func main() {
	// Create a client
	var err error
	client, err = elastic.NewClient(elastic.SetURL("http://localhost:9200"))
	//deleteIndex(INDEX_NAME)
	//createIndex(INDEX_NAME)
	if err != nil {
		log.Fatal(err)
	}

	//log.Println(reflect.TypeOf(client))
	// Define the document
	plateNumber := "DZA-567"
	uuid5 := getUUID(plateNumber)
	doc := Car{
		//UUID5:       uuid5,
		PlateNumber: plateNumber,
		Owner:       "KZ",
		ValidUntil:  "2024-01-01",
		Data:        []string{"macska", "egér", "kutya"},
	}

	// Index the document
	indexResponse, err := client.Index().
		Index(INDEX_NAME).
		BodyJson(doc).
		Id(uuid5).
		Do(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Indexed document %s to index %s\n", indexResponse.Id, indexResponse.Index)
	searchAll()
	search()
}

func search() {
	query := elastic.NewBoolQuery().Should(
		//elastic.NewRegexpQuery("rendszam", ".*DZ.*"),
		//elastic.NewRegexpQuery("tulajdonos", ".*KZ.*"),
		elastic.NewRegexpQuery("adatok", ".*acsk.*"),
	)

	searchResult, err := client.Search().
		Index(INDEX_NAME).
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

func deleteIndex(indexName string) {
	deleteIndex, err := client.DeleteIndex(indexName).Do(context.Background())
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

func getUUID(name string) string {
	namespaceDNS := uuid.NameSpaceDNS
	uuidV5 := uuid.NewSHA1(namespaceDNS, []byte(name))
	return uuidV5.String()
}

func searchAll() {
	// Initialize scrolling over documents
	scroll := client.Scroll(INDEX_NAME).Size(100) // Adjust size as needed
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

func createIndex(indexName string) {
	mapping := `{
		"settings": {
			"number_of_shards": 1,
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
	createIndex, err := client.CreateIndex(indexName).BodyString(mapping).Do(context.Background())
	if err != nil {
		log.Fatalf("Failed to create index: %s", err)
	}
	if !createIndex.Acknowledged {
		log.Fatal("Create index not acknowledged")
	}

	fmt.Println("Index created successfully")
}
