import sys
import pymongo
from pymongo import MongoClient
INDEXES = ['trip_miles', 'trip_seconds', 'payment.fare', 'trip_start_timestamp', 'trip_end_timestamp', 
    [('trip_miles', pymongo.ASCENDING), ('trip_seconds', pymongo.ASCENDING)]]

if __name__ == '__main__':
    drop_index = False 
    if len(sys.argv) > 1:
        if sys.argv[1] == "drop":
            drop_index = True

    client = MongoClient('localhost', 27017, username='root', password='password')
    db = client["taxiRides"]
    rides_collection = db["rides"]

    for index in INDEXES:
        if drop_index:
            print(f"Dropping index {index}")
            try:
                if isinstance(index, str):
                    rides_collection.drop_index(index)
                else:
                    names = [name for name, order in index]
                    rides_collection.drop_index("_".join(names))
                    
            except Exception as e:
                print("\tIndex does not exists")
                print(f"\t{e}")                
        else:
            print(f"Creating index {index}")
            try:
                if isinstance(index, str):
                    rides_collection.create_index(index, name=index)
                else:
                    names = [name for name, order in index]
                    rides_collection.create_index(index, name=("_".join(names)))
            except Exception as e:
                print("\tIndex already exists")
                print(f"\t{e}")                
            