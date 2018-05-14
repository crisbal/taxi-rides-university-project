from pymongo import MongoClient

if __name__ == '__main__':
    client = MongoClient('localhost', 27017, username='root', password='password')
    print("Dropping database")
    client.drop_database("taxiRides")
    db = client["taxiRides"]
    db.add_user('root', 'password', roles=[{'role':'readWrite','db':'taxiRides'}])
    #db.drop()
    """collections = ["rides"]
    for collection in collections:
        print(f"Dropping collection {collection}")
        db[collection].drop()
        print("\tDropped")"""
    
