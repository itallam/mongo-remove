# author: Isabel Tallam

import pymongo
import time

mongo_location = ... # ip address
mongo_db = ... # mongo db name
mongo_collection = ... # mongo collection name
query_items_to_remove = {...} # query of all items to be removed
query_next_batch = {.....} # query to find next batch of items (subset of query_items_to_remove to use batch)
next = ... # start of batch if needed

increment = 5000 # items per batch
total = 0 # counter of removed items

# connect to mongo & db
client = pymongo.MongoClient(mongo_location, 27017)
db = client.mongo_db
print 'Connected to ' + str(db)

# remove 1000 objects at a time
cursor = db.mongo_collection.find(query_items_to_remove)
remaining = cursor.count()
print 'Found ' + str(remaining) + ' documents. Starting to remove documents with id < ' + str(next) 

# while remaining > 0 :
while remaining > 0 :

   # remove documents
   result = db.mongo_collection.remove(query_next_batch)
   cursor = db.mongo_collection.find(query_items_to_remove)
   remaining = cursor.count()
   print 'Found ' + str(remaining) + ' documents. Starting to remove documents with id < ' + str(next)

   # wait for 10 sec before removing next batch
   time.sleep(2)
   next += increment
   total += increment

print 'COMPLETE'
