client = MongoClient('localhost:27017')
# Database 
db = client["Strava"]
# Collection
collection_topic = db["TOPIC"]

# Insert document in the collection
for el in diz:
    collection_topic.insert_one(el)



