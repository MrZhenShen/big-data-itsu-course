from pymongo import MongoClient
import json

# Connect to the MongoDB client
client = MongoClient('mongodb://localhost:27017/')

# Database name
db = client['cinema']

# Collection name
collection_name = 'movies'

# Check if the collection exists
if collection_name in db.list_collection_names():
    print(f"Collection '{collection_name}' already exists.")
else:
    # Create a new collection
    db.create_collection(collection_name)
    print(f"Collection '{collection_name}' has been created.")

# Path to your JSON file
file_path = './movies.json'

# Load JSON data from the file
with open(file_path, 'r') as file:
    data = json.load(file)

# Insert data into the collection
if isinstance(data, list):
    db[collection_name].insert_many(data)
else:
    db[collection_name].insert_one(data)

print("Data has been inserted into the MongoDB collection.")
