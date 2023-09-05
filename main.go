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
	apiURL         = "https://jsonplaceholder.typicode.com/posts" // API URL to fetch data from
	mongoURI       = "mongodb://localhost:27017"                  // MongoDB connection URI
	databaseName   = "test"                                       // Name of the database
	collectionName = "posts"                                      // Name of the collection
)

// Function to call the API and fetch data
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

// Function to compute the hash of a given data
func computeHash(data map[string]interface{}) string {
	bytes, err := json.Marshal(data)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(bytes)
	return hex.EncodeToString(sum[:])
}

func main() {

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	// Create a new MongoDB client
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
	}

	defer client.Disconnect(ctx)

	// Call the API and fetch data
	data, err := callAPI()
	if err != nil {
		log.Fatalf("Failed to call API: %v", err)
	}

	// Get the collection from the database
	coll := client.Database(databaseName).Collection(collectionName)

	// Process each item in the fetched data
	for _, v := range data {
		item, ok := v.(map[string]interface{})
		if !ok {
			log.Printf("Unexpected data format: %v", v)
			continue
		}

		// Compute the hash of the item
		hash := computeHash(item)

		// Define the filter, update, and options for the update operation
		filter := bson.M{"hash": hash}
		update := bson.M{"$set": bson.M{"fetched_at": time.Now(), "data": item}}
		upsert := true
		updateOptions := options.Update().SetUpsert(upsert)

		// Perform the update operation on the collection
		_, err := coll.UpdateOne(ctx, filter, update, updateOptions)
		if err != nil {
			log.Printf("Failed to update or insert item: %v", err)
		}
	}

	log.Println("All data processed!")
}
