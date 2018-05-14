from pymongo import MongoClient

if __name__ == '__main__':
    client = MongoClient('localhost', 27017, username='root', password='password')
    db = client["taxiRides"]
    
    collections = ["rides"]
    for collection in collections:
        print(f"Dropping collection {collection}")
        db[collection].drop()
        print("\tDropped")
        