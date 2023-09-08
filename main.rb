require 'net/http'
require 'json'
require 'digest'
require 'mongo'

# Constants
API_URL = 'https://jsonplaceholder.typicode.com/posts' # URL of the API to fetch data from
MONGO_URI = 'mongodb://localhost:27017/test' # MongoDB connection URI
COLLECTION_NAME = 'ruby_posts' # Name of the collection in MongoDB

# Function to call the API and fetch data
def call_api
  uri = URI(API_URL)
  response = Net::HTTP.get(uri)
  JSON.parse(response)
rescue StandardError => e
  puts "Failed to call API: #{e.message}"
  []
end

# Function to compute the hash of a given data
def compute_hash(data)
  Digest::SHA256.hexdigest(JSON.generate(data))
rescue StandardError => e
  puts "Failed to compute hash: #{e.message}"
  ''
end

begin
  client = Mongo::Client.new(MONGO_URI) # Create a new MongoDB client using the connection URI
  collection = client[COLLECTION_NAME] # Get the collection with the specified name

  data = call_api # Call the API and fetch the data

  data.each do |item|
    hash = compute_hash(item) # Compute the hash of each item in the data
    filter = { 'hash' => hash } # Create a filter to find documents with the same hash
    update = { '$set' => { 'fetched_at' => Time.now, 'data' => item } } # Create an update to set the fetched_at and data fields

    collection.find_one_and_update(filter, update, upsert: true) # Find a document matching the filter and update it, or insert a new document if no match is found
  end

  puts 'All data processed!'
rescue StandardError => e
  puts "Error: #{e.message}"
ensure
  client&.close
end
