package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	apiURL         = "https://jsonplaceholder.typicode.com/posts"
	mongoURI       = "mongodb://localhost:27017"
	databaseName   = "test"
	collectionName = "posts"
)

func callAPI() ([]interface{}, error) {
	resp, err := http.Get(apiURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var data []interface{}
	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&data)
	return data, err
}

func computeHash(data map[string]interface{}) string {
	bytes, err := json.Marshal(data)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(bytes)
	return hex.EncodeToString(sum[:])
}

func main() {
	client, err := mongo.NewClient(options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	err = client.Connect(ctx)
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
	}

	defer client.Disconnect(ctx)

	data, err := callAPI()
	if err != nil {
		log.Fatalf("Failed to call API: %v", err)
	}

	coll := client.Database(databaseName).Collection(collectionName)

	for _, v := range data {
		item, ok := v.(map[string]interface{})
		if !ok {
			log.Printf("Unexpected data format: %v", v)
			continue
		}

		hash := computeHash(item)

		filter := bson.M{"hash": hash}
		update := bson.M{"$set": bson.M{"fetched_at": time.Now(), "data": item}}
		upsert := true
		updateOptions := options.Update().SetUpsert(upsert)

		_, err := coll.UpdateOne(ctx, filter, update, updateOptions)
		if err != nil {
			log.Printf("Failed to update or insert item: %v", err)
		}
	}

	log.Println("All data processed!")
}
